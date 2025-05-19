from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional, Dict, Any, Set
from pydantic import BaseModel
from fastapi.concurrency import run_in_threadpool
from app.services.hf_datasets import (
    get_dataset_commits,
    get_dataset_files,
    get_file_url,
    get_datasets_page_from_zset,
)
from app.services.redis_client import sync_cache_get
import logging
import time
from fastapi.responses import JSONResponse

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
    meta = sync_cache_get("hf:datasets:meta")
    last_update = meta["last_update"] if meta and "last_update" in meta else None
    total_items = meta["total_items"] if meta and "total_items" in meta else 0
    warming_up = not bool(total_items)
    return CacheStatus(last_update=last_update, total_items=total_items, warming_up=warming_up)

@router.get("/", response_model=None)
async def list_datasets(
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=1000),
    search: Optional[str] = None,
    impact_level: Optional[str] = Query(None, description="Filter by impact level (e.g., high, medium, low, not_available)"),
    min_size: Optional[int] = Query(None, description="Minimum dataset size in bytes"),
):
    # Fetch from Sorted Set + Hash for deduplication and performance
    offset = (page - 1) * limit
    result = await run_in_threadpool(get_datasets_page_from_zset, offset, limit, search)
    items = result["items"]
    # Server-side filtering
    if impact_level:
        items = [item for item in items if item.get("impact_level") == impact_level]
    if min_size is not None:
        def parse_size(val):
            try:
                return int(val)
            except Exception:
                return 0
        items = [item for item in items if parse_size(item.get("size_bytes")) >= min_size]
    # Paginate after filtering (deduplication is automatic)
    start = 0
    end = limit
    page_items = items[start:end]
    return JSONResponse(content={"total": len(items), "items": page_items, "page": page, "limit": limit})

@router.get("/{dataset_id:path}/commits", response_model=List[CommitInfo])
async def get_commits(dataset_id: str):
    """
    Get commit history for a dataset.
    """
    try:
        return await run_in_threadpool(get_dataset_commits, dataset_id)
    except Exception as e:
        log.error(f"Error fetching commits for {dataset_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Could not fetch commits: {e}")

@router.get("/{dataset_id:path}/files", response_model=List[str])
async def list_files(dataset_id: str):
    """
    List files in a dataset.
    """
    try:
        return await run_in_threadpool(get_dataset_files, dataset_id)
    except Exception as e:
        log.error(f"Error listing files for {dataset_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Could not list files: {e}")

@router.get("/{dataset_id:path}/file-url")
async def get_file_url_endpoint(dataset_id: str, filename: str = Query(...), revision: Optional[str] = None):
    """
    Get download URL for a file in a dataset.
    """
    url = await run_in_threadpool(get_file_url, dataset_id, filename, revision)
    return {"download_url": url}

@router.get("/meta")
def get_datasets_meta():
    import redis, json
    redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
    meta = redis_client.get("hf:datasets:meta")
    return json.loads(meta) if meta else {}
