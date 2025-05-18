# app/services/follows.py

"""Dataset follow management via Supabase."""

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


async def _to_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


async def follow_dataset(user_id: str, dataset_id: str) -> None:
    """Insert a ``(user_id, dataset_id)`` row if missing."""

    def _insert():
        db = require_supabase()
        # ``upsert`` ensures idempotency
        db.table("dataset_follows").upsert(
            {"user_id": user_id, "dataset_id": dataset_id}, on_conflict="user_id"
        ).execute()

    await _to_thread(_insert)


async def unfollow_dataset(user_id: str, dataset_id: str) -> None:
    """Remove a follow relation if it exists."""

    def _delete():
        db = require_supabase()
        (
            db.table("dataset_follows")
            .delete()
            .eq("user_id", user_id)
            .eq("dataset_id", dataset_id)
            .execute()
        )

    await _to_thread(_delete)


async def is_following(user_id: str, dataset_id: str) -> bool:
    """Return ``True`` if the user follows the dataset."""

    def _check() -> bool:
        db = require_supabase()
        res = (
            db.table("dataset_follows")
            .select("id")
            .eq("user_id", user_id)
            .eq("dataset_id", dataset_id)
            .limit(1)
            .execute()
        )
        return bool(res.data)

    return await _to_thread(_check)


async def list_followed_datasets(user_id: str, limit: int | None = None, offset: int = 0) -> List[str]:
    """Return dataset ids the user follows, newest first."""

    def _select() -> List[str]:
        db = require_supabase()
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

    return await _to_thread(_select)
