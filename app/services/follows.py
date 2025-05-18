from __future__ import annotations

from typing import List, Optional

from app.supabase import require_supabase

__all__ = [
    "follow_dataset",
    "unfollow_dataset",
    "is_following",
    "list_followed_datasets",
]

_TABLE = "dataset_follows"


def follow_dataset(user_id: str, dataset_id: str) -> None:
    """Insert a follow row. Duplicate follows are ignored."""
    client = require_supabase()
    client.table(_TABLE).upsert({"user_id": user_id, "dataset_id": dataset_id}).execute()


def unfollow_dataset(user_id: str, dataset_id: str) -> None:
    """Remove a follow row if it exists."""
    client = require_supabase()
    client.table(_TABLE).delete().eq("user_id", user_id).eq("dataset_id", dataset_id).execute()


def is_following(user_id: str, dataset_id: str) -> bool:
    client = require_supabase()
    res = (
        client.table(_TABLE)
        .select("id")
        .eq("user_id", user_id)
        .eq("dataset_id", dataset_id)
        .limit(1)
        .execute()
    )
    return bool(res.data)


def list_followed_datasets(
    user_id: str,
    *,
    limit: Optional[int] = None,
    offset: int = 0,
) -> List[str]:
    client = require_supabase()
    query = (
        client.table(_TABLE)
        .select("dataset_id")
        .eq("user_id", user_id)
        .order("followed_at", desc=True)
    )
    if limit is not None:
        query = query.range(offset, offset + limit - 1)
    elif offset:
        query = query.range(offset, None)
    res = query.execute()
    return [row["dataset_id"] for row in res.data]
