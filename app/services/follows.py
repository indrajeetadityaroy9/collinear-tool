# app/services/follows.py

from typing import List, Optional
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dataset_follows import DatasetFollow

__all__ = [
    "follow_dataset",
    "unfollow_dataset",
    "is_following",
    "list_followed_datasets",
]

async def follow_dataset(
    session: AsyncSession,
    *,
    user_id: int,
    dataset_id: str,
) -> DatasetFollow:
    """
    Insert a *(user, dataset)* row – **silently succeeds** if it already exists.

    Returns the row (freshly inserted or pre-existing).
    """

    q = select(DatasetFollow).where(
        DatasetFollow.user_id == user_id,
        DatasetFollow.dataset_id == dataset_id,
    )
    if existing := (await session.execute(q)).scalar_one_or_none():
        return existing

    follow = DatasetFollow(user_id=user_id, dataset_id=dataset_id)
    session.add(follow)
    try:
        await session.commit()
    except IntegrityError:          
        await session.rollback()
        follow = (await session.execute(q)).scalar_one()

    return follow


# ────────────────────────────────────────────────────────────────────
# Delete (no-op if missing) ─────────────────────────────────────────
# ────────────────────────────────────────────────────────────────────
async def unfollow_dataset(
    session: AsyncSession,
    *,
    user_id: int,
    dataset_id: str,
) -> None:
    stmt = delete(DatasetFollow).where(
        DatasetFollow.user_id == user_id,
        DatasetFollow.dataset_id == dataset_id,
    )
    await session.execute(stmt)
    await session.commit()


async def is_following(
    session: AsyncSession,
    *,
    user_id: int,
    dataset_id: str,
) -> bool:
    q = select(DatasetFollow.id).where(
        DatasetFollow.user_id == user_id,
        DatasetFollow.dataset_id == dataset_id,
    )
    return (await session.execute(q)).scalar_one_or_none() is not None


async def list_followed_datasets(
    session: AsyncSession,
    *,
    user_id: int,
    limit: Optional[int] = None,
    offset: int = 0,
) -> List[DatasetFollow]:
    """
    Return the user’s followed datasets (most-recent first).

    `limit=None`  →  unlimited.
    """
    q = (
        select(DatasetFollow)
        .where(DatasetFollow.user_id == user_id)
        .order_by(DatasetFollow.followed_at.desc())
        .offset(offset)
    )
    if limit:
        q = q.limit(limit)

    result = await session.execute(q)
    return list(result.scalars().all())
