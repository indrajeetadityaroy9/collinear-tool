from fastapi import APIRouter, HTTPException, Query, Depends, Response

from app.api.dependencies import current_user, User
from app.services.follows import follow_dataset, unfollow_dataset
from app.services.hf_datasets import (
    list_datasets_async,
    commit_history_async,
    list_repo_files_async,
    get_file_download_url_async,
)

router = APIRouter(prefix="/datasets", tags=["datasets"])

@router.get("")
async def list_datasets_endpoint(
    limit: int | None = Query(None, ge=1),
    search: str | None = None,
    with_size: bool = Query(False, description="Include total repo size (bytes)"),
):
    return await list_datasets_async(
        limit=limit,
        search=search,
        include_size=with_size,
    )

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
async def follow_endpoint(dataset_id: str, user: User = Depends(current_user)):
    await follow_dataset(user.id, dataset_id)
    return Response(status_code=204)

@router.delete("/{dataset_id:path}/follow", status_code=204)
async def unfollow_endpoint(dataset_id: str, user: User = Depends(current_user)):
    await unfollow_dataset(user.id, dataset_id)
    return Response(status_code=204)
