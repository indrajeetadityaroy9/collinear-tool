from fastapi import APIRouter, HTTPException, Query, Depends, Response, Request, BackgroundTasks, Path
from typing import List, Dict, Any, Optional
import uuid
from datetime import datetime, timezone
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.core.dependencies import get_current_user, get_core_settings
from app.services.follows import follow_dataset, unfollow_dataset
from app.services.hf_datasets import (
    list_datasets_async,
    commit_history_async,
    list_repo_files_async,
    get_file_download_url_async,
    determine_impact_level,
    get_dataset_impact_async,
)
from app.services.dataset_impacts import (
    save_dataset_impact,
    get_stored_dataset_impact,
    is_impact_assessment_stale,
    list_datasets_by_impact,
    populate_impact_assessments,
    get_batch_status
)
from app.services.dataset_combine import (
    create_combined_dataset,
    get_combined_dataset,
    list_combined_datasets,
    estimate_combined_impact
)
from app.schemas.dataset import (
    ImpactAssessment, 
    ImpactLevel, 
    DatasetMetrics, 
    Dataset, 
    DatasetCreate, 
    DatasetUpdate,
    DatasetCombineRequest,
    CombinedDataset,
    list_combined_files
)
from app.core.config import settings, Settings
from app.services.redis_client import cache_get, cache_set, generate_cache_key
from app.schemas.user import User
from app.supabase import get_new_supabase_client

# Define a paginated response model
class PaginatedResponse(BaseModel):
    items: List[Dict[str, Any]]
    count: int
    limit: Optional[int] = None
    offset: Optional[int] = None

router = APIRouter(prefix="/datasets", tags=["datasets"])

# Cache settings
DATASET_LIST_CACHE_TTL = 60 * 5  # 5 minutes
DATASET_LIST_CACHE_KEY = "datasets:list"

@router.get("")
async def list_datasets_endpoint(
    limit: int | None = Query(10, ge=1),
    offset: int | None = Query(0, ge=0),
    search: str | None = None,
    with_size: bool = Query(False, description="Include total repo size (bytes)"),
    with_impact: bool = Query(False, description="Include dataset impact assessment"),
    use_stored_impacts: bool = Query(True, description="Use stored impact assessments from database when available"),
    skip_cache: bool = Query(False, description="Skip cache and force new data fetch")
) -> PaginatedResponse:
    """
    List datasets with optional filtering and impact assessments.
    
    This endpoint supports:
    - Pagination with limit and offset parameters
    - Text-based search filtering
    - Including impact assessments
    - Using pre-calculated impact assessments from database
    - Caching results for better performance
    """
    # Generate cache key based on request parameters
    if settings.ENABLE_REDIS_CACHE and not skip_cache:
        cache_key = generate_cache_key(
            DATASET_LIST_CACHE_KEY,
            str(limit),
            str(offset),
            search or "all",
            str(with_size),
            str(with_impact),
            str(use_stored_impacts)
        )
        
        # Try to get from cache
        cached_data = await cache_get(cache_key)
        if cached_data:
            return PaginatedResponse(**cached_data)
    
    # Fetch fresh data
    results = await list_datasets_async(
        limit=limit,
        offset=offset,
        search=search,
        include_size=with_size,
        include_impact=with_impact,
        use_stored_impacts=use_stored_impacts,
        count_total=True,  # Request total count
    )
    
    # If results is a dict with 'items' and 'count', use that structure
    if isinstance(results, dict) and 'items' in results and 'count' in results:
        # Make sure items is a list
        if not isinstance(results['items'], list):
            results['items'] = []
        response_data = results
    else:
        # Otherwise, assume it's just a list of items and estimate the count
        # Make sure results is not a coroutine by ensuring it's already awaited
        if not isinstance(results, list):
            # If somehow we got a coroutine, await it first (should not happen normally)
            try:
                import inspect
                if inspect.iscoroutine(results):
                    results = await results
            except Exception as e:
                # In case of any error, return an empty list for safety
                results = []
        
        response_data = {
            "items": results,
            "count": len(results) + (offset or 0),  # Simple estimation, handle None offset
        }
    
    # Add pagination metadata
    response_data["limit"] = limit
    response_data["offset"] = offset
    
    # Create paginated response
    paginated_response = PaginatedResponse(**response_data)
    
    # Cache results
    if settings.ENABLE_REDIS_CACHE and not skip_cache:
        await cache_set(cache_key, paginated_response.dict(), expire=DATASET_LIST_CACHE_TTL)
    
    return paginated_response

@router.get("/{dataset_id:path}/impact", response_model=ImpactAssessment)
async def dataset_impact_endpoint(
    dataset_id: str,
    request: Request,
    force_refresh: bool = Query(False, description="Force a fresh calculation of impact assessment")
) -> ImpactAssessment:
    """
    Get the impact assessment for a specific dataset.
    
    Returns a comprehensive impact assessment based on:
    - Dataset size (from dataset-viewer API or file count estimation)
    - Popularity metrics (downloads and likes)
    
    Impact levels are categorized as:
    - Low: Small datasets with low popularity
    - Medium: Medium-sized datasets or datasets with moderate popularity
    - High: Large datasets or highly popular datasets
    
    By default, returns cached assessment if available (for up to 7 days).
    Use force_refresh=true to recalculate the impact assessment.
    """
    # Get JWT from request
    jwt = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        jwt = auth_header.split(" ", 1)[1]
    
    # Try Redis cache first for the fastest response
    if settings.ENABLE_REDIS_CACHE and not force_refresh:
        cache_key = generate_cache_key("dataset:impact", dataset_id)
        cached_impact = await cache_get(cache_key)
        if cached_impact:
            # Convert cached data to Pydantic model
            metrics = DatasetMetrics(
                size_bytes=cached_impact["metrics"]["size_bytes"],
                file_count=cached_impact["metrics"]["file_count"],
                downloads=cached_impact["metrics"]["downloads"],
                likes=cached_impact["metrics"]["likes"]
            )
            
            return ImpactAssessment(
                dataset_id=dataset_id,
                impact_level=cached_impact["impact_level"],
                assessment_method=cached_impact["assessment_method"],
                metrics=metrics,
                thresholds=cached_impact["thresholds"]
            )
    
    # Check for stored assessment in database if not forcing refresh
    if not force_refresh:
        stored_impact = await get_stored_dataset_impact(dataset_id, jwt)
        if stored_impact and not await is_impact_assessment_stale(stored_impact):
            # Convert stored data to Pydantic model
            metrics = DatasetMetrics(
                size_bytes=stored_impact["size_bytes"],
                file_count=stored_impact["file_count"],
                downloads=stored_impact["downloads"],
                likes=stored_impact["likes"]
            )
            
            # Rebuild thresholds (these are constants from the service)
            impact_data = await get_dataset_impact_async(dataset_id)
            
            assessment = ImpactAssessment(
                dataset_id=dataset_id,
                impact_level=ImpactLevel(stored_impact["impact_level"]),
                assessment_method=stored_impact["assessment_method"],
                metrics=metrics,
                thresholds=impact_data["thresholds"]
            )
            
            # Cache in Redis for faster future access
            if settings.ENABLE_REDIS_CACHE:
                cache_key = generate_cache_key("dataset:impact", dataset_id)
                await cache_set(cache_key, assessment.dict(), expire=DATASET_LIST_CACHE_TTL)
                
            return assessment
    
    # Calculate fresh impact assessment
    impact_data = await get_dataset_impact_async(dataset_id)
    
    # Store the new assessment in the database
    await save_dataset_impact(dataset_id, impact_data, jwt)
    
    # Convert to Pydantic model
    metrics = DatasetMetrics(
        size_bytes=impact_data["metrics"]["size_bytes"],
        file_count=impact_data["metrics"]["file_count"],
        downloads=impact_data["metrics"]["downloads"],
        likes=impact_data["metrics"]["likes"]
    )
    
    assessment = ImpactAssessment(
        dataset_id=dataset_id,
        impact_level=impact_data["impact_level"],
        assessment_method=impact_data["assessment_method"],
        metrics=metrics,
        thresholds=impact_data["thresholds"]
    )
    
    return assessment

@router.get("/by-impact/{impact_level}", response_model=List[str])
async def datasets_by_impact_endpoint(
    impact_level: ImpactLevel,
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    skip_cache: bool = Query(False, description="Skip cache and force database query")
) -> List[str]:
    """
    Get a list of dataset IDs with the specified impact level.
    
    This endpoint retrieves datasets that have been previously assessed and 
    stored in the database with the given impact level.
    """
    # Get JWT from request
    jwt = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        jwt = auth_header.split(" ", 1)[1]
        
    return await list_datasets_by_impact(impact_level, limit, jwt, use_cache=not skip_cache)

@router.post("/populate-impacts", status_code=202)
async def populate_impacts_endpoint(
    request: Request,
    dataset_ids: List[str],
    background_tasks: BackgroundTasks,
    force_refresh: bool = Query(False, description="Force refresh of dataset impacts even when cached"),
) -> Dict[str, Any]:
    """
    Populate impact assessments for a batch of datasets.
    
    This endpoint is intended for admin use to pre-calculate impact assessments
    for datasets to avoid doing it on-demand.
    
    It runs as a background task to prevent timeouts on large batches.
    
    You can optionally force a refresh of all dataset impacts with the force_refresh parameter.
    """
    # Get JWT from request
    jwt = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        jwt = auth_header.split(" ", 1)[1]
    
    # Process batch with the enhanced service
    result = await populate_impact_assessments(dataset_ids, jwt)
    
    return result

@router.get("/batch/{batch_id}", response_model=Dict[str, Any])
async def get_batch_status_endpoint(
    batch_id: str = Path(..., description="The batch ID to check status for")
) -> Dict[str, Any]:
    """
    Get the status of a batch dataset impact assessment job.
    
    Returns information about the batch processing status including:
    - Overall status (pending, processing, complete, error)
    - Progress information
    - Results if the batch is complete
    """
    return await get_batch_status(batch_id)

@router.post("/combined", response_model=CombinedDataset, status_code=202)
async def create_combined_dataset_endpoint(
    request: Request,
    combine_request: DatasetCombineRequest,
    user: User = Depends(get_current_user)
) -> CombinedDataset:
    """
    Create a new combined dataset from multiple source datasets.
    
    This endpoint allows users to combine multiple datasets using different strategies:
    - "merge": Combines all datasets (union)
    - "intersect": Only keeps data present in all datasets (intersection)
    - "filter": Applies filters to a base dataset
    
    The process runs asynchronously and the combined dataset will be available
    when processing is complete.
    """
    # Get JWT from request for database operations
    jwt = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        jwt = auth_header.split(" ", 1)[1]
    
    # Create the combined dataset
    combined_dataset = await create_combined_dataset(
        combine_request=combine_request,
        user_id=user.id,
        jwt=jwt
    )
    
    return combined_dataset

@router.get("/combined", response_model=List[CombinedDataset])
async def list_combined_datasets_endpoint(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    user: User = Depends(get_current_user)
) -> List[CombinedDataset]:
    """
    List combined datasets created by the current user.
    
    This endpoint returns a list of combined datasets with their status
    and basic information.
    """
    # Get JWT from request for database operations
    jwt = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        jwt = auth_header.split(" ", 1)[1]
    
    # List combined datasets
    combined_datasets = await list_combined_datasets(
        user_id=user.id,
        limit=limit,
        jwt=jwt
    )
    
    return combined_datasets

@router.get("/combined/{combined_id}", response_model=CombinedDataset)
async def get_combined_dataset_endpoint(
    combined_id: str,
    request: Request,
    user: User = Depends(get_current_user)
) -> CombinedDataset:
    """
    Get details of a specific combined dataset.
    
    This endpoint returns detailed information about a combined dataset,
    including its processing status and metrics.
    """
    # Get JWT from request for database operations
    jwt = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        jwt = auth_header.split(" ", 1)[1]
    
    # Get combined dataset
    combined_dataset = await get_combined_dataset(combined_id, jwt)
    
    if not combined_dataset:
        raise HTTPException(status_code=404, detail="Combined dataset not found")
    
    return combined_dataset

@router.post("/estimate-combined-impact", response_model=Dict[str, Any])
async def estimate_combined_impact_endpoint(
    request: Request,
    source_datasets: List[str],
    strategy: str = Query("merge", description="Combination strategy: merge, intersect, or filter")
) -> Dict[str, Any]:
    """
    Estimate the impact level and metrics of a combined dataset without creating it.
    
    This endpoint is useful for understanding the potential impact of
    combining datasets before actually creating the combination.
    """
    # Validate strategy
    valid_strategies = ["merge", "intersect", "filter"]
    if strategy not in valid_strategies:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid strategy. Must be one of: {', '.join(valid_strategies)}"
        )
    
    try:
        # Estimate impact
        impact_level, metrics = await estimate_combined_impact(source_datasets, strategy)
        
        # Return the estimate
        return {
            "impact_level": impact_level,
            "metrics": {
                "size_bytes": metrics.size_bytes,
                "file_count": metrics.file_count,
                "downloads": metrics.downloads,
                "likes": metrics.likes
            },
            "strategy": strategy,
            "source_datasets": source_datasets
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error estimating impact: {str(e)}")

@router.get("/{dataset_id:path}/commits")
async def commit_history_endpoint(dataset_id: str):
    """Get the commit history for a dataset."""
    try:
        commits = await commit_history_async(dataset_id)
        return commits
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching commit history: {str(e)}")

@router.get("/{dataset_id:path}/files")
async def list_files_endpoint(dataset_id: str):
    """List files in a dataset."""
    try:
        files = await list_repo_files_async(dataset_id)
        return files
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")

@router.get("/{dataset_id:path}/file-url")
async def file_url_endpoint(
    dataset_id: str,
    filename: str,
    revision: str | None = None,
):
    """Get download URL for a file in a dataset."""
    try:
        url = await get_file_download_url_async(
            dataset_id=dataset_id,
            filename=filename,
            revision=revision
        )
        return {"url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting file URL: {str(e)}")

@router.post("/{dataset_id:path}/follow", status_code=204)
async def follow_endpoint(request: Request, dataset_id: str, user: User = Depends(get_current_user)):
    """Follow a dataset."""
    await follow_dataset(user.id, dataset_id)
    return Response(status_code=204)
    
@router.delete("/{dataset_id:path}/follow", status_code=204)
async def unfollow_endpoint(request: Request, dataset_id: str, user: User = Depends(get_current_user)):
    """Unfollow a dataset."""
    await unfollow_dataset(user.id, dataset_id) 
    return Response(status_code=204)
    
@router.post("/", response_model=Dataset, status_code=201)
async def create_dataset(
    dataset_in: DatasetCreate, 
    current_user: User = Depends(get_current_user),
    settings: Settings = Depends(get_core_settings)
):
    """Create a new dataset."""
    try:
        # Create a new dataset with current timestamp
        new_dataset = {
            "name": dataset_in.name,
            "description": dataset_in.description,
            "tags": dataset_in.tags,
            "owner_id": current_user.id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Get Supabase client
        supabase = get_new_supabase_client()
        
        # Insert dataset into database
        result = supabase.table("datasets").insert(new_dataset).execute()
        
        if not result.data or len(result.data) == 0:
            raise HTTPException(status_code=500, detail="Failed to create dataset")
            
        # Return the created dataset
        created_dataset = result.data[0]
        
        return Dataset(
            id=created_dataset["id"],
            name=created_dataset["name"],
            description=created_dataset["description"],
            tags=created_dataset["tags"],
            owner_id=created_dataset["owner_id"],
            created_at=created_dataset["created_at"],
            updated_at=created_dataset["updated_at"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating dataset: {str(e)}")

@router.get("/user", response_model=List[Dataset])
async def list_datasets(
    skip: int = 0, 
    limit: int = Query(default=10, le=100),
    current_user: User = Depends(get_current_user)
):
    """List datasets owned by current user."""
    try:
        # Get Supabase client
        supabase = get_new_supabase_client()
        
        # Query datasets owned by current user
        result = supabase.table("datasets") \
            .select("*") \
            .eq("owner_id", current_user.id) \
            .range(skip, skip + limit - 1) \
            .execute()
        
        if not result.data:
            return []
            
        # Return list of datasets
        return [
            Dataset(
                id=item["id"],
                name=item["name"],
                description=item["description"],
                tags=item["tags"],
                owner_id=item["owner_id"],
                created_at=item["created_at"],
                updated_at=item["updated_at"]
            ) for item in result.data
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing datasets: {str(e)}")

@router.get("/user/{dataset_id}", response_model=Dataset)
async def get_dataset(
    dataset_id: int, 
    current_user: User = Depends(get_current_user)
):
    """Get a specific dataset owned by the current user."""
    try:
        # Get Supabase client
        supabase = get_new_supabase_client()
        
        # Query specific dataset by ID and owner
        result = supabase.table("datasets") \
            .select("*") \
            .eq("id", dataset_id) \
            .eq("owner_id", current_user.id) \
            .execute()
        
        if not result.data or len(result.data) == 0:
            raise HTTPException(status_code=404, detail="Dataset not found")
            
        dataset = result.data[0]
        
        return Dataset(
            id=dataset["id"],
            name=dataset["name"],
            description=dataset["description"],
            tags=dataset["tags"],
            owner_id=dataset["owner_id"],
            created_at=dataset["created_at"],
            updated_at=dataset["updated_at"]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving dataset: {str(e)}")

@router.put("/user/{dataset_id}", response_model=Dataset)
async def update_dataset(
    dataset_id: int, 
    dataset_in: DatasetUpdate, 
    current_user: User = Depends(get_current_user)
):
    """Update a dataset owned by the current user."""
    try:
        # Get Supabase client
        supabase = get_new_supabase_client()
        
        # First check if dataset exists and belongs to user
        check_result = supabase.table("datasets") \
            .select("id") \
            .eq("id", dataset_id) \
            .eq("owner_id", current_user.id) \
            .execute()
        
        if not check_result.data or len(check_result.data) == 0:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Prepare update data, only including fields that are provided
        update_data = {"updated_at": datetime.now(timezone.utc).isoformat()}
        
        if dataset_in.name is not None:
            update_data["name"] = dataset_in.name
            
        if dataset_in.description is not None:
            update_data["description"] = dataset_in.description
            
        if dataset_in.tags is not None:
            update_data["tags"] = dataset_in.tags
        
        # Update dataset
        result = supabase.table("datasets") \
            .update(update_data) \
            .eq("id", dataset_id) \
            .execute()
        
        if not result.data or len(result.data) == 0:
            raise HTTPException(status_code=500, detail="Failed to update dataset")
            
        updated_dataset = result.data[0]
        
        return Dataset(
            id=updated_dataset["id"],
            name=updated_dataset["name"],
            description=updated_dataset["description"],
            tags=updated_dataset["tags"],
            owner_id=updated_dataset["owner_id"],
            created_at=updated_dataset["created_at"],
            updated_at=updated_dataset["updated_at"]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating dataset: {str(e)}")

@router.delete("/user/{dataset_id}", status_code=204)
async def delete_dataset(
    dataset_id: int, 
    current_user: User = Depends(get_current_user)
):
    """Delete a dataset owned by the current user."""
    try:
        # Get Supabase client
        supabase = get_new_supabase_client()
        
        # First check if dataset exists and belongs to user
        check_result = supabase.table("datasets") \
            .select("id") \
            .eq("id", dataset_id) \
            .eq("owner_id", current_user.id) \
            .execute()
        
        if not check_result.data or len(check_result.data) == 0:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Delete dataset
        supabase.table("datasets") \
            .delete() \
            .eq("id", dataset_id) \
            .execute()
        
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting dataset: {str(e)}")

@router.get("/combined/{combined_id}/files", response_model=List[str])
async def list_combined_files_endpoint(
    combined_id: str,
    request: Request,
    user: User = Depends(get_current_user)
) -> List[str]:
    """
    List files in a combined dataset.
    
    This endpoint returns a list of files in the combined dataset storage.
    """
    # Get JWT from request for authentication
    jwt = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        jwt = auth_header.split(" ", 1)[1]
    
    return await list_combined_files(combined_id, jwt)
