from fastapi import APIRouter, HTTPException, Query, Depends, Response, Request, BackgroundTasks, Path
from typing import List, Dict, Any, Optional
import uuid
from datetime import datetime, timezone

from app.api.dependencies import current_user, User
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
from app.schemas.dataset import ImpactAssessment, ImpactLevel, DatasetMetrics
from app.core.config import settings
from app.services.redis_client import cache_get, cache_set, generate_cache_key

router = APIRouter(prefix="/datasets", tags=["datasets"])

# Cache settings
DATASET_LIST_CACHE_TTL = 60 * 5  # 5 minutes
DATASET_LIST_CACHE_KEY = "datasets:list"

@router.get("")
async def list_datasets_endpoint(
    limit: int | None = Query(None, ge=1),
    search: str | None = None,
    with_size: bool = Query(False, description="Include total repo size (bytes)"),
    with_impact: bool = Query(False, description="Include dataset impact assessment"),
    use_stored_impacts: bool = Query(True, description="Use stored impact assessments from database when available"),
    skip_cache: bool = Query(False, description="Skip cache and force new data fetch")
) -> List[Dict[str, Any]]:
    """
    List datasets with optional filtering and impact assessments.
    
    This endpoint supports:
    - Pagination with limit parameter
    - Text-based search filtering
    - Including impact assessments
    - Using pre-calculated impact assessments from database
    - Caching results for better performance
    """
    # Generate cache key based on request parameters
    if settings.enable_redis_cache and not skip_cache:
        cache_key = generate_cache_key(
            DATASET_LIST_CACHE_KEY,
            str(limit),
            search or "all",
            str(with_size),
            str(with_impact),
            str(use_stored_impacts)
        )
        
        # Try to get from cache
        cached_data = await cache_get(cache_key)
        if cached_data:
            return cached_data
    
    # Fetch fresh data
    results = await list_datasets_async(
        limit=limit,
        search=search,
        include_size=with_size,
        include_impact=with_impact,
        use_stored_impacts=use_stored_impacts,
    )
    
    # Cache results
    if settings.enable_redis_cache and not skip_cache:
        await cache_set(cache_key, results, expire=DATASET_LIST_CACHE_TTL)
    
    return results

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
    if settings.enable_redis_cache and not force_refresh:
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
            if settings.enable_redis_cache:
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
) -> Dict[str, Any]:
    """
    Populate impact assessments for a batch of datasets.
    
    This endpoint is intended for admin use to pre-calculate impact assessments
    for datasets to avoid doing it on-demand.
    
    It runs as a background task to prevent timeouts on large batches.
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

@router.get("/{dataset_id:path}/commits")
async def commit_history_endpoint(dataset_id: str):
    return await commit_history_async(dataset_id)

@router.get("/{dataset_id:path}/files")
async def list_files_endpoint(dataset_id: str):
    return await list_repo_files_async(dataset_id)

@router.get("/{dataset_id:path}/file-url")
async def file_url_endpoint(
    dataset_id: str,
    filename: str,
    revision: str | None = None,
):
    try:
        url = await get_file_download_url_async(
            dataset_id, filename, revision=revision
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"download_url": url}

@router.post("/{dataset_id:path}/follow", status_code=204)
async def follow_endpoint(request: Request, dataset_id: str, user: User = Depends(current_user)):
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    await follow_dataset(user.id, dataset_id, jwt)
    return Response(status_code=204)

@router.delete("/{dataset_id:path}/follow", status_code=204)
async def unfollow_endpoint(request: Request, dataset_id: str, user: User = Depends(current_user)):
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    await unfollow_dataset(user.id, dataset_id, jwt)
    return Response(status_code=204)
