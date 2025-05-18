"""Dataset follow management using Supabase tables."""

from __future__ import annotations

import asyncio
from typing import List
import logging
import sys

from app.supabase import require_supabase

__all__ = [
    "follow_dataset",
    "unfollow_dataset",
    "is_following",
    "list_followed_datasets",
]


async def follow_dataset(user_id: str, dataset_id: str, jwt: str) -> None:
    """Ensure ``user_id`` follows ``dataset_id`` using a direct insert with the authenticated user's client."""
    logging.info(f"Following: user_id={user_id}, dataset_id={dataset_id}, jwt={jwt[:10]}...")
    db = require_supabase(jwt)
    logging.info(f"Inserting follow: dataset_id={dataset_id}")
    result = await asyncio.to_thread(
        lambda: db.table("dataset_follows").insert({"dataset_id": dataset_id}).execute()
    )
    logging.info(f"Insert result: {result}")


async def unfollow_dataset(user_id: str, dataset_id: str, jwt: str) -> None:
    """Remove a follow relationship if present."""
    try:
        print(f"DEBUG: unfollow_dataset called with user_id={user_id}, dataset_id={dataset_id}", flush=True)
        sys.stdout.flush()
        logging.info(f"Unfollowing: user_id={user_id}, dataset_id={dataset_id}, jwt={jwt[:10]}...")
        db = require_supabase(jwt)
        logging.info(f"Unfollowing: user_id={user_id}, dataset_id={dataset_id}")
        result = await asyncio.to_thread(
            lambda: db.table("dataset_follows")
            .delete()
            .eq("user_id", user_id)
            .eq("dataset_id", dataset_id)
            .execute()
        )
        print(f"Unfollow result: {result}", flush=True)
        print(f"Unfollow result data: {getattr(result, 'data', None)}", flush=True)
        print(f"Unfollow result error: {getattr(result, 'error', None)}", flush=True)
        print(f"Unfollow result __dict__: {getattr(result, '__dict__', str(result))}", flush=True)
        sys.stdout.flush()
        if not getattr(result, 'data', None):
            raise Exception("Not Found")
    except Exception as e:
        print(f"Exception in unfollow_dataset: {e}", flush=True)
        sys.stdout.flush()
        raise


async def is_following(user_id: str, dataset_id: str, jwt: str) -> bool:
    """Return ``True`` if ``user_id`` follows ``dataset_id``."""

    db = require_supabase(jwt)

    res = await asyncio.to_thread(
        lambda: db.table("dataset_follows")
        .select("id")
        .eq("user_id", user_id)
        .eq("dataset_id", dataset_id)
        .limit(1)
        .execute()
    )
    return bool(res.data)


async def list_followed_datasets(user_id: str, limit: int | None = None, offset: int = 0, jwt: str = None) -> List[str]:
    """Return dataset IDs ``user_id`` follows, newest first."""

    db = require_supabase(jwt)

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
