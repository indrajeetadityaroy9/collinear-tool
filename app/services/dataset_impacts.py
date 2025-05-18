"""Dataset impact assessment storage and retrieval."""

from __future__ import annotations

import asyncio
import logging
import uuid
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple

from app.schemas.dataset import ImpactLevel
from app.supabase import require_supabase
from app.core.config import settings
from app.services.redis_client import (
    cache_get, 
    cache_set, 
    cache_invalidate, 
    cache_invalidate_pattern,
    enqueue_task,
    get_task_status,
    get_task_result,
    generate_cache_key
)

log = logging.getLogger(__name__)

# Cache keys and TTLs
DATASET_IMPACT_KEY_PREFIX = "dataset:impact"
IMPACT_LIST_KEY_PREFIX = "datasets:by_impact"
IMPACT_CACHE_TTL = 60 * 60 * 24  # 24 hours
IMPACT_LIST_CACHE_TTL = 60 * 60  # 1 hour

# Task queue names
DATASET_IMPACT_QUEUE = "dataset_impacts"

async def save_dataset_impact(dataset_id: str, impact_data: Dict[str, Any], jwt: str = None) -> None:
    """
    Save dataset impact assessment to the database.
    
    Args:
        dataset_id: The unique identifier of the dataset
        impact_data: Impact assessment data with metrics and impact level
        jwt: Optional JWT for authenticated access
    """
    try:
        # Use service key for authenticated access
        db = require_supabase(jwt)
        
        # Convert enum to string if needed
        impact_level = impact_data["impact_level"]
        if hasattr(impact_level, "value"):
            impact_level = impact_level.value
        
        # Replace None/NULL values with 0 for numeric metrics
        metrics = impact_data["metrics"].copy()
        for key in ["size_bytes", "file_count", "downloads", "likes"]:
            if metrics.get(key) is None:
                metrics[key] = 0
                log.info(f"Replaced NULL value with 0 for {key} in dataset {dataset_id}")
        
        # Prepare data for insertion
        impact_record = {
            "dataset_id": dataset_id,
            "impact_level": impact_level,
            "size_bytes": metrics["size_bytes"],
            "file_count": metrics["file_count"],
            "downloads": metrics["downloads"],
            "likes": metrics["likes"],
            "assessment_method": impact_data["assessment_method"],
            "last_assessed_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Insert or update the impact assessment
        log.info(f"Saving impact assessment for dataset {dataset_id}")
        result = await asyncio.to_thread(
            lambda: db.table("dataset_impacts")
            .upsert(impact_record)
            .execute()
        )
        log.info(f"Successfully saved impact for {dataset_id}: {impact_level}")
        
        # Invalidate cache for this dataset and impact level lists
        if settings.enable_redis_cache:
            cache_key = generate_cache_key(DATASET_IMPACT_KEY_PREFIX, dataset_id)
            await cache_invalidate(cache_key)
            
            # Also invalidate cached lists of datasets by impact
            await cache_invalidate_pattern(f"{IMPACT_LIST_KEY_PREFIX}:*")
            
            # Cache the new impact data
            await cache_set(cache_key, impact_data, expire=IMPACT_CACHE_TTL)
        
        return result
    except Exception as e:
        log.error(f"Error saving impact for {dataset_id}: {e}")
        # Don't re-raise the exception to avoid breaking the API response
        # Just log it and continue

async def get_stored_dataset_impact(dataset_id: str, jwt: str = None, use_cache: bool = True) -> Optional[Dict[str, Any]]:
    """
    Retrieve a stored dataset impact assessment.
    
    Args:
        dataset_id: The unique identifier of the dataset
        jwt: Optional JWT for authenticated access
        use_cache: Whether to use Redis cache if available
        
    Returns:
        Dictionary with impact assessment data if found, None otherwise
    """
    # Try to get from cache first if enabled
    if settings.enable_redis_cache and use_cache:
        cache_key = generate_cache_key(DATASET_IMPACT_KEY_PREFIX, dataset_id)
        cached_data = await cache_get(cache_key)
        if cached_data:
            log.debug(f"Cache hit for dataset impact: {dataset_id}")
            return cached_data
    
    # Otherwise get from database
    db = require_supabase(jwt)
    
    try:
        result = await asyncio.to_thread(
            lambda: db.table("dataset_impacts")
            .select("*")
            .eq("dataset_id", dataset_id)
            .execute()
        )
        
        if not result.data or len(result.data) == 0:
            return None
            
        impact_data = result.data[0]
        
        # Cache the result if caching is enabled
        if settings.enable_redis_cache and use_cache:
            cache_key = generate_cache_key(DATASET_IMPACT_KEY_PREFIX, dataset_id)
            await cache_set(cache_key, impact_data, expire=IMPACT_CACHE_TTL)
            
        return impact_data
    except Exception as e:
        log.error(f"Error retrieving impact for {dataset_id}: {e}")
        return None

async def is_impact_assessment_stale(stored_impact: Dict[str, Any], max_age_days: int = 7) -> bool:
    """
    Check if a stored impact assessment is stale (older than max_age_days).
    
    Args:
        stored_impact: Stored impact assessment data
        max_age_days: Maximum age in days before considered stale
        
    Returns:
        True if assessment is stale, False otherwise
    """
    if not stored_impact or "last_assessed_at" not in stored_impact:
        return True
        
    try:
        last_assessed = datetime.fromisoformat(stored_impact["last_assessed_at"])
        max_age = timedelta(days=max_age_days)
        return (datetime.now(timezone.utc) - last_assessed) > max_age
    except (ValueError, TypeError):
        # If date parsing fails, consider it stale
        return True

async def list_datasets_by_impact(impact_level: ImpactLevel, limit: int = 100, jwt: str = None, use_cache: bool = True) -> list[str]:
    """
    List dataset IDs with a specific impact level.
    
    Args:
        impact_level: The impact level to filter by
        limit: Maximum number of results to return
        jwt: Optional JWT for authenticated access
        use_cache: Whether to use Redis cache if available
        
    Returns:
        List of dataset IDs with the specified impact level
    """
    # Try to get from cache first if enabled
    if settings.enable_redis_cache and use_cache:
        cache_key = generate_cache_key(IMPACT_LIST_KEY_PREFIX, impact_level.value, str(limit))
        cached_data = await cache_get(cache_key)
        if cached_data:
            log.debug(f"Cache hit for datasets by impact: {impact_level.value}")
            return cached_data
    
    # Otherwise get from database
    db = require_supabase(jwt)
    
    try:
        result = await asyncio.to_thread(
            lambda: db.table("dataset_impacts")
            .select("dataset_id")
            .eq("impact_level", impact_level.value)
            .order("last_assessed_at", desc=True)
            .limit(limit)
            .execute()
        )
        
        if not result.data:
            return []
            
        dataset_ids = [item["dataset_id"] for item in result.data]
        
        # Cache the result if caching is enabled
        if settings.enable_redis_cache and use_cache:
            cache_key = generate_cache_key(IMPACT_LIST_KEY_PREFIX, impact_level.value, str(limit))
            await cache_set(cache_key, dataset_ids, expire=IMPACT_LIST_CACHE_TTL)
            
        return dataset_ids
    except Exception as e:
        log.error(f"Error retrieving datasets by impact level {impact_level}: {e}")
        return []

async def populate_impact_assessments(dataset_ids: List[str], jwt: str = None) -> Dict[str, Any]:
    """
    Populate impact assessments for a batch of datasets.
    
    This function is useful for background jobs that pre-calculate impact assessments
    for datasets to avoid doing it on-demand.
    
    Args:
        dataset_ids: List of dataset IDs to assess
        jwt: Optional JWT for authenticated access
        
    Returns:
        Dictionary with batch information and task ID
    """
    # Check if batch is small enough to process synchronously
    if len(dataset_ids) <= 5:
        # Process small batches synchronously
        from app.services.hf_datasets import get_dataset_impact_async
        
        results = {}
        errors = []
        
        # Process datasets in parallel with a limit on concurrency
        semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests
        
        async def process_dataset(dataset_id: str):
            async with semaphore:
                try:
                    log.info(f"Calculating impact for dataset: {dataset_id}")
                    impact_data = await get_dataset_impact_async(dataset_id)
                    await save_dataset_impact(dataset_id, impact_data, jwt)
                    results[dataset_id] = impact_data["impact_level"].value
                    log.info(f"Saved impact for {dataset_id}: {impact_data['impact_level'].value}")
                except Exception as e:
                    log.error(f"Error calculating impact for {dataset_id}: {e}")
                    errors.append((dataset_id, str(e)))
        
        # Create tasks for all datasets
        tasks = [process_dataset(dataset_id) for dataset_id in dataset_ids]
        
        # Run all tasks
        await asyncio.gather(*tasks)
        
        if errors:
            log.warning(f"Encountered {len(errors)} errors during impact assessment population")
        
        return {
            "processed_synchronously": True,
            "datasets": len(dataset_ids),
            "successful": len(results),
            "errors": len(errors),
            "results": results
        }
    else:
        # For larger batches, use the worker queue
        if not settings.enable_redis_cache:
            # Fallback to synchronous processing if Redis isn't available
            log.warning("Redis not available, processing batch synchronously")
            return await populate_impact_assessments(dataset_ids[:5], jwt)
            
        # Split into batches based on configuration
        batch_size = settings.worker_batch_size
        
        # Generate a batch ID
        batch_id = f"impact-batch-{uuid.uuid4()}"
        
        # Prepare task payload
        payload = {
            "batch_id": batch_id,
            "dataset_ids": dataset_ids,
            "jwt": jwt,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Enqueue the batch job
        success = await enqueue_task(DATASET_IMPACT_QUEUE, batch_id, payload)
        
        if not success:
            log.error(f"Failed to enqueue batch job {batch_id}")
            return {
                "status": "error",
                "message": "Failed to enqueue batch job",
                "batch_id": batch_id
            }
            
        return {
            "status": "accepted",
            "message": f"Processing {len(dataset_ids)} datasets in the background",
            "batch_id": batch_id,
            "datasets": len(dataset_ids)
        }

async def get_batch_status(batch_id: str) -> Dict[str, Any]:
    """
    Get status of a batch impact assessment job.
    
    Args:
        batch_id: The batch ID to check
        
    Returns:
        Dictionary with batch status information
    """
    if not settings.enable_redis_cache:
        return {
            "status": "unknown",
            "message": "Redis not available, cannot check batch status"
        }
        
    # Check task status
    status = await get_task_status(DATASET_IMPACT_QUEUE, batch_id)
    
    if not status:
        return {
            "status": "unknown",
            "message": f"No status found for batch {batch_id}"
        }
        
    # If task is complete, get the result
    if status == "complete":
        result = await get_task_result(DATASET_IMPACT_QUEUE, batch_id)
        if result:
            return {
                "status": "complete",
                "batch_id": batch_id,
                **result
            }
            
    # Check for progress information
    progress = await cache_get(f"batch:progress:{batch_id}")
    
    if progress:
        return {
            "status": "processing" if status == "pending" else status,
            "batch_id": batch_id,
            **progress
        }
        
    # Default response
    return {
        "status": status,
        "batch_id": batch_id,
        "message": f"Batch {batch_id} is {status}"
    }

# Export the functions
__all__ = [
    "save_dataset_impact",
    "get_stored_dataset_impact",
    "is_impact_assessment_stale",
    "list_datasets_by_impact",
    "populate_impact_assessments",
    "get_batch_status"
] 