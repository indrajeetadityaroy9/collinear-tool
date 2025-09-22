"""Dataset-related API routes."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.integrations.hf_datasets import (
    fetch_and_cache_all_datasets,
    get_dataset_commits_async,
    get_dataset_files_async,
    get_datasets_page_from_cache,
    get_file_url_async,
)
from app.integrations.redis_client import cache_get

router = APIRouter(prefix="/datasets", tags=["datasets"])
log = logging.getLogger(__name__)


class CommitInfo(BaseModel):
    id: str
    title: Optional[str]
    message: Optional[str]
    author: Optional[Dict[str, Any]]
    date: Optional[str]


class CacheStatus(BaseModel):
    last_update: Optional[str]
    total_items: int
    warming_up: bool


@router.get("/cache-status", response_model=CacheStatus)
async def cache_status() -> CacheStatus:
    """Return metadata about the cached Hugging Face dataset corpus."""
    meta = await cache_get("hf:datasets:meta")
    last_update = meta.get("last_update") if isinstance(meta, dict) else None
    total_items = int(meta.get("total_items", 0)) if isinstance(meta, dict) else 0
    warming_up = total_items == 0
    return CacheStatus(last_update=last_update, total_items=total_items, warming_up=warming_up)


@router.get("/")
async def list_datasets(
    limit: int = Query(10, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, description="Filter by dataset id or description"),
    sort_by: Optional[str] = Query(None, description="Field to sort by (e.g. downloads, likes)"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order: asc or desc"),
) -> Dict[str, Any]:
    """Return a page of cached datasets with optional search and sorting."""
    result, status = get_datasets_page_from_cache(1_000_000, 0)
    if status != 200:
        return JSONResponse(result, status_code=status)

    items = result["items"]
    if search:
        lowered = search.lower()
        items = [
            dataset
            for dataset in items
            if lowered in (dataset.get("id", "").lower() + " " + str(dataset.get("description", "")).lower())
        ]

    if sort_by:
        reverse = sort_order == "desc"
        items = sorted(items, key=lambda dataset: dataset.get(sort_by) or 0, reverse=reverse)

    total = len(items)
    page = items[offset : offset + limit]
    total_pages = (total + limit - 1) // limit if total else 0
    current_page = (offset // limit) + 1 if total else 0
    next_page = current_page + 1 if offset + limit < total else None
    prev_page = current_page - 1 if current_page > 1 else None

    return {
        "total": total,
        "current_page": current_page,
        "total_pages": total_pages,
        "next_page": next_page,
        "prev_page": prev_page,
        "items": page,
        "warming_up": total == 0,
    }


@router.get("/{dataset_id:path}/commits", response_model=List[CommitInfo])
async def get_commits(dataset_id: str) -> List[CommitInfo]:
    try:
        return await get_dataset_commits_async(dataset_id)
    except Exception as exc:
        log.error("Error fetching commits for %s: %s", dataset_id, exc)
        raise HTTPException(status_code=404, detail=f"Could not fetch commits: {exc}") from exc


@router.get("/{dataset_id:path}/files", response_model=List[str])
async def list_files(dataset_id: str) -> List[str]:
    try:
        return await get_dataset_files_async(dataset_id)
    except Exception as exc:
        log.error("Error listing files for %s: %s", dataset_id, exc)
        raise HTTPException(status_code=404, detail=f"Could not list files: {exc}") from exc


@router.get("/{dataset_id:path}/file-url")
async def get_file_url_endpoint(dataset_id: str, filename: str = Query(...), revision: Optional[str] = None) -> Dict[str, str]:
    url = await get_file_url_async(dataset_id, filename, revision)
    return {"download_url": url}


@router.get("/meta")
async def get_datasets_meta() -> Dict[str, Any]:
    meta = await cache_get("hf:datasets:meta")
    return meta if isinstance(meta, dict) else {}


@router.post("/refresh-cache")
def refresh_cache() -> Dict[str, Any]:
    token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        return JSONResponse({"error": "HUGGINGFACEHUB_API_TOKEN not set"}, status_code=500)
    cached = fetch_and_cache_all_datasets(token)
    return {"status": "ok", "cached": cached}
