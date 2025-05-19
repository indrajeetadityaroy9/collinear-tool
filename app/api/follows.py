from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status, Request, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

from app.services import follows as service
from app.core.dependencies import get_current_user, User


router = APIRouter(
    prefix="/follows",
    tags=["follows"],
)


# ───────────────────────────────────────── models ──────────────────────────────────────────
class FollowIn(BaseModel):
    dataset_id: str


class FollowOut(BaseModel):
    dataset_id: str
    storage_bucket_id: str | None = None
    storage_folder_path: str | None = None


class StorageFileOut(BaseModel):
    name: str
    size: int | None = None
    created_at: str | None = None
    updated_at: str | None = None
    id: str | None = None


# ───────────────────────────────────────── endpoints ────────────────────────────────────────
@router.get("", response_model=list[FollowOut])
async def list_my_follows(
    request: Request,
    user: User = Depends(get_current_user),
):
    """
    Return the dataset IDs the authenticated user is following, with storage references.
    """
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    follows = await service.list_followed_datasets_with_storage(user.id, jwt=jwt)
    return follows


@router.post(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def follow_dataset(
    request: Request,
    payload: FollowIn,
    user: User = Depends(get_current_user),
):
    """
    Follow a dataset. 204 No Content on success.
    """
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    try:
        await service.follow_dataset(user.id, payload.dataset_id, jwt)
    except Exception as exc:  # Supabase client raises generic `Exception`
        raise HTTPException(400, detail=str(exc)) from exc


@router.delete(
    "/{dataset_id:path}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def unfollow_dataset(
    request: Request,
    dataset_id: str,
    user: User = Depends(get_current_user),
):
    """
    Un-follow a dataset. 204 No Content whether or not the relationship existed.
    """
    print("DEBUG: unfollow endpoint called", flush=True)
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    await service.unfollow_dataset(user.id, dataset_id, jwt)


@router.post(
    "/{dataset_id:path}/download",
    status_code=status.HTTP_202_ACCEPTED,
)
async def download_dataset_to_storage(
    request: Request,
    dataset_id: str,
    background_tasks: BackgroundTasks,
    user: User = Depends(get_current_user),
):
    """
    Download all files from the dataset to user's storage bucket.
    
    This is an asynchronous operation that runs in the background.
    Returns 202 Accepted immediately.
    """
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    
    # First check if the user is following this dataset
    is_following = await service.is_following(user.id, dataset_id, jwt)
    if not is_following:
        raise HTTPException(
            status_code=404, 
            detail=f"You are not following dataset {dataset_id}"
        )
    
    # Get storage location for this followed dataset
    follows = await service.list_followed_datasets_with_storage(user.id, jwt=jwt)
    follow = next((f for f in follows if f["dataset_id"] == dataset_id), None)
    
    if not follow or not follow.get("storage_bucket_id") or not follow.get("storage_folder_path"):
        raise HTTPException(
            status_code=400,
            detail="Storage information missing for this followed dataset"
        )
    
    # Start the download process in the background
    background_tasks.add_task(
        service.download_dataset_to_storage,
        user_id=user.id,
        dataset_id=dataset_id, 
        storage_bucket_id=follow["storage_bucket_id"],
        storage_folder_path=follow["storage_folder_path"],
        jwt=jwt
    )
    
    return {"message": "Download initiated", "dataset_id": dataset_id}


@router.get(
    "/{dataset_id:path}/files",
    response_model=List[StorageFileOut]
)
async def list_storage_files(
    request: Request,
    dataset_id: str,
    user: User = Depends(get_current_user),
):
    """
    List files in the storage bucket for a followed dataset.
    """
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    
    # First check if the user is following this dataset
    is_following = await service.is_following(user.id, dataset_id, jwt)
    if not is_following:
        raise HTTPException(
            status_code=404, 
            detail=f"You are not following dataset {dataset_id}"
        )
    
    # Get storage location for this followed dataset
    follows = await service.list_followed_datasets_with_storage(user.id, jwt=jwt)
    follow = next((f for f in follows if f["dataset_id"] == dataset_id), None)
    
    if not follow or not follow.get("storage_bucket_id") or not follow.get("storage_folder_path"):
        raise HTTPException(
            status_code=400,
            detail="Storage information missing for this followed dataset"
        )
    
    files = await service.list_storage_files(
        storage_bucket_id=follow["storage_bucket_id"],
        storage_folder_path=follow["storage_folder_path"],
        jwt=jwt
    )
    
    return files


@router.get(
    "/{dataset_id:path}/file-url",
)
async def get_storage_file_url(
    request: Request,
    dataset_id: str,
    filename: str,
    user: User = Depends(get_current_user),
):
    """
    Get a download URL for a file in the storage bucket.
    """
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    
    # First check if the user is following this dataset
    is_following = await service.is_following(user.id, dataset_id, jwt)
    if not is_following:
        raise HTTPException(
            status_code=404, 
            detail=f"You are not following dataset {dataset_id}"
        )
    
    # Get storage location for this followed dataset
    follows = await service.list_followed_datasets_with_storage(user.id, jwt=jwt)
    follow = next((f for f in follows if f["dataset_id"] == dataset_id), None)
    
    if not follow or not follow.get("storage_bucket_id") or not follow.get("storage_folder_path"):
        raise HTTPException(
            status_code=400,
            detail="Storage information missing for this followed dataset"
        )
    
    url = await service.get_storage_file_url(
        storage_bucket_id=follow["storage_bucket_id"],
        storage_folder_path=follow["storage_folder_path"],
        filename=filename,
        jwt=jwt
    )
    
    return {"url": url}

