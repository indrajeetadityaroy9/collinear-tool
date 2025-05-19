"""Service for combining datasets from multiple sources."""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

from app.schemas.dataset import (
    ImpactLevel, 
    DatasetMetrics, 
    CombinedDataset, 
    DatasetCombineRequest
)
from app.services.hf_datasets import (
    get_dataset_impact_async,
    determine_impact_level,
    determine_impact_level_by_criteria
)
from app.services.dataset_impacts import (
    get_stored_dataset_impact
)
from app.supabase import get_new_supabase_client
from app.core.config import settings
from app.services.redis_client import (
    cache_get,
    cache_set,
    cache_invalidate,
    enqueue_task,
    get_task_status,
    generate_cache_key
)

# Import Celery tasks conditionally to avoid circular imports
try:
    from app.tasks.dataset_tasks import process_combined_dataset
    CELERY_AVAILABLE = True
except ImportError:
    CELERY_AVAILABLE = False

log = logging.getLogger(__name__)

# Cache settings
COMBINED_DATASET_CACHE_TTL = 60 * 60 * 24  # 24 hours
COMBINE_JOB_CACHE_TTL = 60 * 60 * 24 * 7  # 7 days

# Supported combination strategies
SUPPORTED_STRATEGIES = ["merge", "intersect", "filter"]

async def create_combined_dataset(
    combine_request: DatasetCombineRequest,
    user_id: str,
    jwt: Optional[str] = None
) -> CombinedDataset:
    """
    Create a new combined dataset from multiple source datasets.
    
    Args:
        combine_request: Request specifying source datasets and combination parameters
        user_id: ID of the user creating the combined dataset
        jwt: Optional JWT token for authentication
    
    Returns:
        CombinedDataset object with initial information
    """
    # Validate combination strategy
    if combine_request.combination_strategy not in SUPPORTED_STRATEGIES:
        raise ValueError(f"Unsupported combination strategy: {combine_request.combination_strategy}. " 
                        f"Supported strategies are: {', '.join(SUPPORTED_STRATEGIES)}")
    
    # Validate source datasets (ensure they exist)
    valid_datasets = []
    for dataset_id in combine_request.source_datasets:
        try:
            # Check if dataset exists by attempting to get its impact assessment
            impact = await get_dataset_impact_async(dataset_id)
            if impact:
                valid_datasets.append(dataset_id)
        except Exception as e:
            log.warning(f"Dataset {dataset_id} could not be validated: {e}")
    
    if not valid_datasets:
        raise ValueError("No valid source datasets provided")
    
    # Create combined dataset record
    combined_id = str(uuid.uuid4())
    folder_path = f"combined/{combined_id}"
    
    # Get bucket ID of 'combined-datasets'
    bucket_id = None
    try:
        bucket_result = await asyncio.to_thread(
            lambda: get_new_supabase_client().from_("storage.buckets")
            .select("id")
            .eq("name", "combined-datasets")
            .execute()
        )
        if bucket_result.data and len(bucket_result.data) > 0:
            bucket_id = bucket_result.data[0]["id"]
    except Exception as bucket_err:
        log.warning(f"Could not get bucket ID for combined-datasets: {bucket_err}")
    
    combined_dataset = CombinedDataset(
        id=combined_id,
        name=combine_request.name,
        description=combine_request.description,
        source_datasets=valid_datasets,
        created_at=datetime.now(timezone.utc),
        created_by=user_id,
        status="processing",
        combination_strategy=combine_request.combination_strategy,
        storage_bucket_id=bucket_id,
        storage_folder_path=folder_path
    )
    
    # Store in database
    db = None
    try:
        db = get_new_supabase_client(settings_override=settings)
        
        # Only set the session if we have a valid JWT (not a test token)
        if jwt and jwt != "test_token" and "." in jwt and len(jwt.split(".")) >= 2:
            db.auth.set_session(access_token=jwt, refresh_token="dummy_refresh_token")
        
        # Convert to dict for database insertion
        db_record = {
            "id": combined_dataset.id,
            "name": combined_dataset.name,
            "description": combined_dataset.description,
            "source_datasets": combined_dataset.source_datasets,
            "created_by": combined_dataset.created_by,
            "combination_strategy": combined_dataset.combination_strategy,
            "status": combined_dataset.status,
            "storage_bucket_id": combined_dataset.storage_bucket_id,
            "storage_folder_path": combined_dataset.storage_folder_path
        }
        
        # Insert record
        result = await asyncio.to_thread(
            lambda: db.table("combined_datasets").insert(db_record).execute()
        )
        
        log.info(f"Created combined dataset {combined_id} with {len(valid_datasets)} source datasets")
        
        # Queue background task to process the combination
        if CELERY_AVAILABLE:
            # Use Celery task if available
            filter_criteria = combine_request.filter_criteria.dict() if combine_request.filter_criteria else None
            task = process_combined_dataset.apply_async(
                args=[combined_id, valid_datasets, combine_request.combination_strategy, filter_criteria, jwt]
            )
            log.info(f"Queued combination task {task.id} for dataset {combined_id}")
        else:
            # Use simple background task
            # This would be implemented in a separate function or as a redis task
            # For now, just mark as "queued" status
            await asyncio.to_thread(
                lambda: db.table("combined_datasets")
                .update({"status": "queued"})
                .eq("id", combined_id)
                .execute()
            )
            log.info(f"Marked dataset {combined_id} as queued for processing")
            
            # Store task info in Redis for later tracking
            task_info = {
                "dataset_id": combined_id,
                "status": "queued",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "source_datasets": valid_datasets,
                "strategy": combine_request.combination_strategy
            }
            cache_key = generate_cache_key("combine:task", combined_id)
            await cache_set(cache_key, task_info, expire=COMBINE_JOB_CACHE_TTL)
        
        return combined_dataset
    
    except Exception as e:
        log.error(f"Error creating combined dataset: {e}")
        raise
    finally:
        if db:
            db.auth.close()

async def estimate_combined_impact(source_dataset_ids: List[str], strategy: str) -> Tuple[ImpactLevel, DatasetMetrics]:
    """
    Estimate the impact level and metrics of a combined dataset before actually creating it.
    
    Args:
        source_dataset_ids: List of dataset IDs to be combined
        strategy: Combination strategy (merge, intersect, filter)
    
    Returns:
        Tuple of (impact_level, metrics) for the potential combined dataset
    """
    # Collect impact data for all source datasets
    impact_data = []
    for dataset_id in source_dataset_ids:
        try:
            # Try to get stored impact first
            stored_impact = await get_stored_dataset_impact(dataset_id)
            if stored_impact:
                impact_data.append(stored_impact)
            else:
                # Fall back to calculating impact
                impact = await get_dataset_impact_async(dataset_id)
                impact_data.append(impact)
        except Exception as e:
            log.warning(f"Could not get impact data for {dataset_id}: {e}")
    
    if not impact_data:
        raise ValueError("Could not determine impact for any of the source datasets")
    
    # Initialize combined metrics
    combined_size = 0
    combined_file_count = 0
    combined_downloads = 0
    combined_likes = 0
    
    # Estimate metrics based on combination strategy
    if strategy == "merge":
        # For merge, we add up sizes and file counts
        for impact in impact_data:
            metrics = impact.get("metrics", {})
            combined_size += metrics.get("size_bytes", 0) or 0
            combined_file_count += metrics.get("file_count", 0) or 0
            
            # For downloads and likes, we use the max (avoid inflating popularity)
            combined_downloads = max(combined_downloads, metrics.get("downloads", 0) or 0)
            combined_likes = max(combined_likes, metrics.get("likes", 0) or 0)
    
    elif strategy == "intersect":
        # For intersect, use minimum values as conservative estimate
        # (actual intersection would be smaller)
        combined_size = min([impact.get("metrics", {}).get("size_bytes", 0) or 0 for impact in impact_data])
        combined_file_count = min([impact.get("metrics", {}).get("file_count", 0) or 0 for impact in impact_data])
        combined_downloads = min([impact.get("metrics", {}).get("downloads", 0) or 0 for impact in impact_data])
        combined_likes = min([impact.get("metrics", {}).get("likes", 0) or 0 for impact in impact_data])
    
    elif strategy == "filter":
        # For filter, use the original dataset size as upper bound, then estimate reduction
        # This is a rough estimate - assume filtering reduces size by 25%
        base_impact = impact_data[0] if impact_data else {}
        base_metrics = base_impact.get("metrics", {})
        reduction_factor = 0.75  # Assume filtering reduces by 25%
        
        combined_size = int((base_metrics.get("size_bytes", 0) or 0) * reduction_factor)
        combined_file_count = int((base_metrics.get("file_count", 0) or 0) * reduction_factor)
        combined_downloads = base_metrics.get("downloads", 0) or 0
        combined_likes = base_metrics.get("likes", 0) or 0
    
    # Calculate impact level based on combined metrics
    impact_level, assessment_method = determine_impact_level_by_criteria(
        combined_size, combined_downloads, combined_likes
    )
    
    # Create metrics object
    metrics = DatasetMetrics(
        size_bytes=combined_size,
        file_count=combined_file_count,
        downloads=combined_downloads,
        likes=combined_likes
    )
    
    return impact_level, metrics

async def get_combined_dataset(combined_id: str, jwt: Optional[str] = None) -> Optional[CombinedDataset]:
    """
    Get a combined dataset by ID.
    
    Args:
        combined_id: ID of the combined dataset
        jwt: Optional JWT token for authentication
    
    Returns:
        CombinedDataset if found, None otherwise
    """
    db = None
    try:
        db = get_new_supabase_client(settings_override=settings)
        
        # Only set the session if we have a valid JWT (not a test token)
        if jwt and jwt != "test_token" and "." in jwt and len(jwt.split(".")) >= 2:
            db.auth.set_session(access_token=jwt, refresh_token="dummy_refresh_token")
        
        result = await asyncio.to_thread(
            lambda: db.table("combined_datasets")
            .select("*")
            .eq("id", combined_id)
            .execute()
        )
        
        if not result.data or len(result.data) == 0:
            return None
        
        # Convert DB record to CombinedDataset model
        record = result.data[0]
        
        # Build metrics if available
        metrics = None
        if record.get("size_bytes") is not None or record.get("file_count") is not None:
            metrics = DatasetMetrics(
                size_bytes=record.get("size_bytes"),
                file_count=record.get("file_count"),
                downloads=record.get("downloads"),
                likes=record.get("likes")
            )
        
        # Parse timestamp
        created_at = datetime.fromisoformat(record["created_at"]) if record.get("created_at") else datetime.now(timezone.utc)
        
        # Convert impact_level to enum if present
        impact_level = None
        if record.get("impact_level"):
            impact_level = ImpactLevel(record["impact_level"])
        
        return CombinedDataset(
            id=record["id"],
            name=record["name"],
            description=record.get("description"),
            source_datasets=record["source_datasets"],
            created_at=created_at,
            created_by=record["created_by"],
            impact_level=impact_level,
            status=record["status"],
            combination_strategy=record["combination_strategy"],
            metrics=metrics,
            storage_bucket_id=record.get("storage_bucket_id"),
            storage_folder_path=record.get("storage_folder_path")
        )
    
    except Exception as e:
        log.error(f"Error retrieving combined dataset {combined_id}: {e}")
        return None
    finally:
        if db:
            db.auth.close()

async def list_combined_datasets(
    user_id: Optional[str] = None,
    limit: int = 100,
    jwt: Optional[str] = None
) -> List[CombinedDataset]:
    """
    List combined datasets, optionally filtered by user.
    
    Args:
        user_id: Optional user ID to filter by (only show datasets created by this user)
        limit: Maximum number of datasets to return
        jwt: Optional JWT token for authentication
    
    Returns:
        List of CombinedDataset objects
    """
    db = None
    try:
        db = get_new_supabase_client(settings_override=settings)
        
        # Only set the session if we have a valid JWT (not a test token)
        if jwt and jwt != "test_token" and "." in jwt and len(jwt.split(".")) >= 2:
            db.auth.set_session(access_token=jwt, refresh_token="dummy_refresh_token")
        
        # Build query
        query = db.table("combined_datasets").select("*").order("created_at", desc=True).limit(limit)
        
        # Filter by user if specified
        if user_id:
            query = query.eq("created_by", user_id)
        
        # Execute query
        result = await asyncio.to_thread(lambda: query.execute())
        
        if not result.data:
            return []
        
        # Convert results to CombinedDataset objects
        combined_datasets = []
        for record in result.data:
            # Build metrics if available
            metrics = None
            if record.get("size_bytes") is not None or record.get("file_count") is not None:
                metrics = DatasetMetrics(
                    size_bytes=record.get("size_bytes"),
                    file_count=record.get("file_count"),
                    downloads=record.get("downloads"),
                    likes=record.get("likes")
                )
            
            # Parse timestamp
            created_at = datetime.fromisoformat(record["created_at"]) if record.get("created_at") else datetime.now(timezone.utc)
            
            # Convert impact_level to enum if present
            impact_level = None
            if record.get("impact_level"):
                impact_level = ImpactLevel(record["impact_level"])
            
            combined_dataset = CombinedDataset(
                id=record["id"],
                name=record["name"],
                description=record.get("description"),
                source_datasets=record["source_datasets"],
                created_at=created_at,
                created_by=record["created_by"],
                impact_level=impact_level,
                status=record["status"],
                combination_strategy=record["combination_strategy"],
                metrics=metrics,
                storage_bucket_id=record.get("storage_bucket_id"),
                storage_folder_path=record.get("storage_folder_path")
            )
            combined_datasets.append(combined_dataset)
        
        return combined_datasets
    
    except Exception as e:
        log.error(f"Error listing combined datasets: {e}")
        return []
    finally:
        if db:
            db.auth.close()

async def update_combined_dataset_status(
    combined_id: str, 
    status: str, 
    metrics: Optional[DatasetMetrics] = None,
    impact_level: Optional[ImpactLevel] = None,
    metadata: Optional[Dict] = None,
    storage_bucket_id: Optional[str] = None,
    storage_folder_path: Optional[str] = None,
    jwt: Optional[str] = None
) -> bool:
    """
    Update the status and optionally metrics of a combined dataset.
    
    Args:
        combined_id: ID of the combined dataset
        status: New status (processing, completed, failed)
        metrics: Optional metrics to update
        impact_level: Optional impact level to update
        metadata: Optional metadata to store as JSONB
        storage_bucket_id: Optional ID of the storage bucket containing dataset files
        storage_folder_path: Optional path to the folder in the bucket
        jwt: Optional JWT token for authentication
    
    Returns:
        True if update successful, False otherwise
    """
    db = None
    try:
        db = get_new_supabase_client(settings_override=settings)
        
        # Only set the session if we have a valid JWT (not a test token)
        if jwt and jwt != "test_token" and "." in jwt and len(jwt.split(".")) >= 2:
            db.auth.set_session(access_token=jwt, refresh_token="dummy_refresh_token")
        
        # Prepare update data
        update_data = {"status": status}
        
        # Add metrics if provided
        if metrics:
            update_data["size_bytes"] = metrics.size_bytes
            update_data["file_count"] = metrics.file_count
            update_data["downloads"] = metrics.downloads
            update_data["likes"] = metrics.likes
        
        # Add impact level if provided
        if impact_level:
            update_data["impact_level"] = impact_level.value if hasattr(impact_level, "value") else impact_level
        
        # Add metadata if provided
        if metadata:
            update_data["metadata"] = metadata
        
        # Add storage references if provided
        if storage_bucket_id:
            update_data["storage_bucket_id"] = storage_bucket_id
        
        if storage_folder_path:
            update_data["storage_folder_path"] = storage_folder_path
        
        # Get bucket ID of 'combined-datasets' if storage folder is provided but not bucket ID
        if storage_folder_path and not storage_bucket_id:
            try:
                bucket_result = await asyncio.to_thread(
                    lambda: db.from_("storage.buckets")
                    .select("id")
                    .eq("name", "combined-datasets")
                    .execute()
                )
                if bucket_result.data and len(bucket_result.data) > 0:
                    update_data["storage_bucket_id"] = bucket_result.data[0]["id"]
            except Exception as bucket_err:
                log.warning(f"Could not get bucket ID for combined-datasets: {bucket_err}")
        
        # Update record
        result = await asyncio.to_thread(
            lambda: db.table("combined_datasets")
            .update(update_data)
            .eq("id", combined_id)
            .execute()
        )
        
        # Clear cache
        cache_key = generate_cache_key("combined:dataset", combined_id)
        await cache_invalidate(cache_key)
        
        log.info(f"Updated combined dataset {combined_id} status to {status}")
        return True
    
    except Exception as e:
        log.error(f"Error updating combined dataset {combined_id}: {e}")
        return False
    finally:
        if db:
            db.auth.close() 