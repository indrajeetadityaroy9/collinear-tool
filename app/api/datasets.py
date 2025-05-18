from fastapi import APIRouter, HTTPException, Query, Depends, Response, Request
from typing import List, Dict, Any
from fastapi import BackgroundTasks

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
    populate_impact_assessments
)
from app.schemas.dataset import ImpactAssessment, ImpactLevel, DatasetMetrics

router = APIRouter(prefix="/datasets", tags=["datasets"])

@router.get("")
async def list_datasets_endpoint(
    limit: int | None = Query(None, ge=1),
    search: str | None = None,
    with_size: bool = Query(False, description="Include total repo size (bytes)"),
    with_impact: bool = Query(False, description="Include dataset impact assessment"),
    use_stored_impacts: bool = Query(True, description="Use stored impact assessments from database when available")
) -> List[Dict[str, Any]]:
    """
    List datasets with optional filtering and impact assessments.
    
    This endpoint supports:
    - Pagination with limit parameter
    - Text-based search filtering
    - Including impact assessments
    - Using pre-calculated impact assessments from database
    """
    return await list_datasets_async(
        limit=limit,
        search=search,
        include_size=with_size,
        include_impact=with_impact,
        use_stored_impacts=use_stored_impacts,
    )

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
    
    # Check for stored assessment if not forcing refresh
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
            
            return ImpactAssessment(
                dataset_id=dataset_id,
                impact_level=ImpactLevel(stored_impact["impact_level"]),
                assessment_method=stored_impact["assessment_method"],
                metrics=metrics,
                thresholds=impact_data["thresholds"]
            )
    
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
        
    return await list_datasets_by_impact(impact_level, limit, jwt)

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
    
    # Add to background tasks
    background_tasks.add_task(populate_impact_assessments, dataset_ids, jwt)
    
    return {
        "message": f"Processing {len(dataset_ids)} datasets in the background",
        "status": "accepted"
    }
