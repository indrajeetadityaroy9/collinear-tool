"""Dataset follow management using Supabase tables."""

from __future__ import annotations

import asyncio
from typing import List

from app.supabase import require_supabase

__all__ = [
    "follow_dataset",
    "unfollow_dataset",
    "is_following",
    "list_followed_datasets",
]


async def follow_dataset(user_id: str, dataset_id: str) -> None:
    """Ensure ``user_id`` follows ``dataset_id``."""

    db = require_supabase()

    await asyncio.to_thread(
        lambda: db.table("dataset_follows")
        .upsert({"user_id": user_id, "dataset_id": dataset_id}, on_conflict="user_id")
        .execute()
    )


async def unfollow_dataset(user_id: str, dataset_id: str) -> None:
    """Remove a follow relationship if present."""

    db = require_supabase()

    await asyncio.to_thread(
        lambda: db.table("dataset_follows")
        .delete()
        .eq("user_id", user_id)
        .eq("dataset_id", dataset_id)
        .execute()
    )


async def is_following(user_id: str, dataset_id: str) -> bool:
    """Return ``True`` if ``user_id`` follows ``dataset_id``."""

    db = require_supabase()

    res = await asyncio.to_thread(
        lambda: db.table("dataset_follows")
        .select("id")
        .eq("user_id", user_id)
        .eq("dataset_id", dataset_id)
        .limit(1)
        .execute()
    )
    return bool(res.data)


async def list_followed_datasets(user_id: str, limit: int | None = None, offset: int = 0) -> List[str]:
    """Return dataset IDs ``user_id`` follows, newest first."""

    db = require_supabase()

    def _query():
        query = (
            db.table("dataset_follows")
            .select("dataset_id")
            .eq("user_id", user_id)
            .order("followed_at", desc=True)
            .offset(offset)
        )
        if limit:
            query = query.limit(limit)
        res = query.execute()
        return [row["dataset_id"] for row in (res.data or [])]

    return await asyncio.to_thread(_query)
