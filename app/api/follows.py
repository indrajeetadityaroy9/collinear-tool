from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db

from app.services import follows as service
from app.api.dependencies import current_user, User


router = APIRouter(
    prefix="/follows",
    tags=["follows"],
)


# ───────────────────────────────────────── models ──────────────────────────────────────────
class FollowIn(BaseModel):
    dataset_id: str


class FollowOut(BaseModel):
    dataset_id: str


# ───────────────────────────────────────── endpoints ────────────────────────────────────────
@router.get("", response_model=list[FollowOut])
async def list_my_follows(
    user: User = Depends(current_user),
    session: AsyncSession = Depends(get_db),
):
    """
    Return the dataset IDs the authenticated user is following.
    """
    ids = await service.list_followed_datasets(user.id)
    return [{"dataset_id": d} for d in ids]


@router.post(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def follow_dataset(
    payload: FollowIn,
    user: User = Depends(current_user),
    session: AsyncSession = Depends(get_db),
):
    """
    Follow a dataset. 204 No Content on success.
    """
    try:
        await service.follow_dataset(user.id, payload.dataset_id)
    except Exception as exc:  # Supabase client raises generic `Exception`
        raise HTTPException(400, detail=str(exc)) from exc


@router.delete(
    "/{dataset_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def unfollow_dataset(
    dataset_id: str,
    user: User = Depends(current_user),
    session: AsyncSession = Depends(get_db),
):
    """
    Un-follow a dataset. 204 No Content whether or not the relationship existed.
    """
    await service.unfollow_dataset(user.id, dataset_id)

