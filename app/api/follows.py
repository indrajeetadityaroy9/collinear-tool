from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.services import follows as service
from app.api.dependencies import current_user   # whatever you already use
                                                # to inject the authenticated
                                                # user ID / object


router = APIRouter(
    prefix="/api/follows",
    tags=["follows"],
)


# ───────────────────────────────────────── models ──────────────────────────────────────────
class FollowIn(BaseModel):
    dataset_id: str


class FollowOut(BaseModel):
    dataset_id: str


# ───────────────────────────────────────── endpoints ────────────────────────────────────────
@router.get("", response_model=list[FollowOut])
async def list_my_follows(user_id: str = Depends(current_user)):
    """
    Return the dataset IDs the authenticated user is following.
    """
    ids = service.list_followed_datasets(user_id)
    return [{"dataset_id": d} for d in ids]


@router.post(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def follow_dataset(payload: FollowIn, user_id: str = Depends(current_user)):
    """
    Follow a dataset. 204 No Content on success.
    """
    try:
        service.follow_dataset(user_id, payload.dataset_id)
    except Exception as exc:  # Supabase client raises generic `Exception`
        raise HTTPException(400, detail=str(exc)) from exc


@router.delete(
    "/{dataset_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def unfollow_dataset(dataset_id: str, user_id: str = Depends(current_user)):
    """
    Un-follow a dataset. 204 No Content whether or not the relationship existed.
    """
    service.unfollow_dataset(user_id, dataset_id)

