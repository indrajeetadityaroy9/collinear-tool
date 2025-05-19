from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional, Dict, Any, Set
from pydantic import BaseModel
from fastapi.concurrency import run_in_threadpool
from app.services.hf_datasets import (
    get_dataset_commits,
    get_dataset_files,
    get_file_url,
    get_datasets_page_from_zset,
    get_dataset_commits_async,
    get_dataset_files_async,
    get_file_url_async,
    get_datasets_page_from_cache,
    fetch_and_cache_all_datasets,
)
from app.services.redis_client import cache_get
import logging
import time
from fastapi.responses import JSONResponse
import os

router = APIRouter(prefix="/datasets", tags=["datasets"])
log = logging.getLogger(__name__)

SIZE_LOW = 100 * 1024 * 1024
SIZE_MEDIUM = 1024 * 1024 * 1024

class DatasetInfo(BaseModel):
    id: str
    name: Optional[str]
    description: Optional[str]
    size_bytes: Optional[int]
    impact_level: Optional[str]
    downloads: Optional[int]
    likes: Optional[int]
    tags: Optional[List[str]]
    class Config:
        extra = "ignore"

class PaginatedDatasets(BaseModel):
    total: int
    items: List[DatasetInfo]

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

def deduplicate_by_id(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    unique_items = []
    for item in items:
        item_id = item.get("id")
        if item_id and item_id not in seen:
            seen.add(item_id)
            unique_items.append(item)
    return unique_items

@router.get("/cache-status", response_model=CacheStatus)
async def cache_status():
    meta = await cache_get("hf:datasets:meta")
    last_update = meta["last_update"] if meta and "last_update" in meta else None
    total_items = meta["total_items"] if meta and "total_items" in meta else 0
    warming_up = not bool(total_items)
    return CacheStatus(last_update=last_update, total_items=total_items, warming_up=warming_up)

@router.get("/", response_model=None)
async def list_datasets(
    limit: int = Query(10, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    search: str = Query(None, description="Search term for dataset id or description"),
    sort_by: str = Query(None, description="Field to sort by (e.g., 'downloads', 'likes', 'created_at')"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
):
    # Fetch the full list from cache
    result, status = get_datasets_page_from_cache(1000000, 0)  # get all for in-memory filtering
    if status != 200:
        return JSONResponse(result, status_code=status)
    items = result["items"]
    # Filtering
    if search:
        items = [d for d in items if search.lower() in (d.get("id", "") + " " + str(d.get("description", "")).lower())]
    # Sorting
    if sort_by:
        items = sorted(items, key=lambda d: d.get(sort_by) or 0, reverse=(sort_order == "desc"))
    # Pagination
    total = len(items)
    page = items[offset:offset+limit]
    total_pages = (total + limit - 1) // limit
    current_page = (offset // limit) + 1
    next_page = current_page + 1 if offset + limit < total else None
    prev_page = current_page - 1 if current_page > 1 else None
    return {
        "total": total,
        "current_page": current_page,
        "total_pages": total_pages,
        "next_page": next_page,
        "prev_page": prev_page,
        "items": page
    }

@router.get("/{dataset_id:path}/commits", response_model=List[CommitInfo])
async def get_commits(dataset_id: str):
    """
    Get commit history for a dataset.
    """
    try:
        return await get_dataset_commits_async(dataset_id)
    except Exception as e:
        log.error(f"Error fetching commits for {dataset_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Could not fetch commits: {e}")

@router.get("/{dataset_id:path}/files", response_model=List[str])
async def list_files(dataset_id: str):
    """
    List files in a dataset.
    """
    try:
        return await get_dataset_files_async(dataset_id)
    except Exception as e:
        log.error(f"Error listing files for {dataset_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Could not list files: {e}")

@router.get("/{dataset_id:path}/file-url")
async def get_file_url_endpoint(dataset_id: str, filename: str = Query(...), revision: Optional[str] = None):
    """
    Get download URL for a file in a dataset.
    """
    url = await get_file_url_async(dataset_id, filename, revision)
    return {"download_url": url}

@router.get("/meta")
async def get_datasets_meta():
    meta = await cache_get("hf:datasets:meta")
    return meta if meta else {}

# Endpoint to trigger cache refresh manually (for admin/testing)
@router.post("/datasets/refresh-cache")
def refresh_cache():
    token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        return JSONResponse({"error": "HUGGINGFACEHUB_API_TOKEN not set"}, status_code=500)
    count = fetch_and_cache_all_datasets(token)
    return {"status": "ok", "cached": count}
