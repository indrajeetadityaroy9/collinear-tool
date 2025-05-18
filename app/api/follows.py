from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status, Request
from pydantic import BaseModel

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
    request: Request,
    user: User = Depends(current_user),
):
    """
    Return the dataset IDs the authenticated user is following.
    """
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    ids = await service.list_followed_datasets(user.id, jwt=jwt)
    return [{"dataset_id": d} for d in ids]


@router.post(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def follow_dataset(
    request: Request,
    payload: FollowIn,
    user: User = Depends(current_user),
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
    user: User = Depends(current_user),
):
    """
    Un-follow a dataset. 204 No Content whether or not the relationship existed.
    """
    print("DEBUG: unfollow endpoint called", flush=True)
    jwt = request.headers.get("authorization", "").split(" ", 1)[1]
    await service.unfollow_dataset(user.id, dataset_id, jwt)

