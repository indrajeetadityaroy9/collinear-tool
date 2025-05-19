import logging
import json
from typing import Any, List, Optional, Dict, Tuple
import requests
from huggingface_hub import HfApi
from app.core.config import settings
from app.schemas.dataset_common import ImpactLevel
from app.services.redis_client import sync_cache_set, sync_cache_get, generate_cache_key, get_redis_sync
import time
import asyncio
import redis
import gzip
from datetime import datetime, timezone
import os
from app.schemas.dataset import ImpactAssessment
from app.schemas.dataset_common import DatasetMetrics
import httpx
import redis.asyncio as aioredis

log = logging.getLogger(__name__)
api = HfApi()
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

# Thresholds for impact categorization
SIZE_THRESHOLD_LOW = 100 * 1024 * 1024  # 100 MB
SIZE_THRESHOLD_MEDIUM = 1024 * 1024 * 1024  # 1 GB
DOWNLOADS_THRESHOLD_LOW = 1000
DOWNLOADS_THRESHOLD_MEDIUM = 10000
LIKES_THRESHOLD_LOW = 10
LIKES_THRESHOLD_MEDIUM = 100

HF_API_URL = "https://huggingface.co/api/datasets"
DATASET_CACHE_TTL = 60 * 60  # 1 hour

# Redis and HuggingFace API setup
REDIS_KEY = "hf:datasets:all:compressed"
REDIS_META_KEY = "hf:datasets:meta"
REDIS_TTL = 60 * 60  # 1 hour

# Impact thresholds (in bytes)
SIZE_LOW = 100 * 1024 * 1024
SIZE_MEDIUM = 1024 * 1024 * 1024

def get_hf_token():
    token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        raise RuntimeError("HUGGINGFACEHUB_API_TOKEN environment variable is not set. Please set it securely.")
    return token

def get_dataset_commits(dataset_id: str, limit: int = 20):
    from huggingface_hub import HfApi
    import logging
    log = logging.getLogger(__name__)
    api = HfApi()
    log.info(f"[get_dataset_commits] Fetching commits for dataset_id={dataset_id}")
    try:
        commits = api.list_repo_commits(repo_id=dataset_id, repo_type="dataset")
        log.info(f"[get_dataset_commits] Received {len(commits)} commits for {dataset_id}")
    except Exception as e:
        log.error(f"[get_dataset_commits] Error fetching commits for {dataset_id}: {e}", exc_info=True)
        raise  # Let the API layer catch and handle this
    result = []
    for c in commits[:limit]:
        try:
            commit_id = getattr(c, "commit_id", "")
            title = getattr(c, "title", "")
            message = getattr(c, "message", title)
            authors = getattr(c, "authors", [])
            author_name = authors[0] if authors and isinstance(authors, list) else ""
            created_at = getattr(c, "created_at", None)
            if created_at:
                if hasattr(created_at, "isoformat"):
                    date = created_at.isoformat()
                else:
                    date = str(created_at)
            else:
                date = ""
            result.append({
                "id": commit_id or "",
                "title": title or message or "",
                "message": message or title or "",
                "author": {"name": author_name, "email": ""},
                "date": date,
            })
        except Exception as e:
            log.error(f"[get_dataset_commits] Error parsing commit: {e} | Commit: {getattr(c, '__dict__', str(c))}", exc_info=True)
    log.info(f"[get_dataset_commits] Returning {len(result)} parsed commits for {dataset_id}")
    return result

def get_dataset_files(dataset_id: str) -> List[str]:
    return api.list_repo_files(repo_id=dataset_id, repo_type="dataset")

def get_file_url(dataset_id: str, filename: str, revision: Optional[str] = None) -> str:
    from huggingface_hub import hf_hub_url
    return hf_hub_url(repo_id=dataset_id, filename=filename, repo_type="dataset", revision=revision)

def get_datasets_page_from_zset(offset: int = 0, limit: int = 10, search: str = None) -> dict:
    import redis
    import json
    redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
    zset_key = "hf:datasets:all:zset"
    hash_key = "hf:datasets:all:hash"
    # Get total count
    total = redis_client.zcard(zset_key)
    # Get dataset IDs for the page
    ids = redis_client.zrange(zset_key, offset, offset + limit - 1)
    # Fetch metadata for those IDs
    if not ids:
        return {"items": [], "count": total}
    items = redis_client.hmget(hash_key, ids)
    # Parse JSON and filter/search if needed
    parsed = []
    for raw in items:
        if not raw:
            continue
        try:
            item = json.loads(raw)
            parsed.append(item)
        except Exception:
            continue
    if search:
        parsed = [d for d in parsed if search.lower() in (d.get("id") or "").lower()]
    return {"items": parsed, "count": total}

async def _fetch_size(session: httpx.AsyncClient, dataset_id: str) -> Optional[int]:
    """Fetch dataset size from the datasets server asynchronously."""
    url = f"https://datasets-server.huggingface.co/size?dataset={dataset_id}"
    try:
        resp = await session.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("size", {}).get("dataset", {}).get("num_bytes_original_files")
    except Exception as e:
        log.warning(f"Could not fetch size for {dataset_id}: {e}")
    return None

async def _fetch_sizes(dataset_ids: List[str]) -> Dict[str, Optional[int]]:
    """Fetch dataset sizes in parallel."""
    results: Dict[str, Optional[int]] = {}
    async with httpx.AsyncClient() as session:
        tasks = {dataset_id: asyncio.create_task(_fetch_size(session, dataset_id)) for dataset_id in dataset_ids}
        for dataset_id, task in tasks.items():
            results[dataset_id] = await task
    return results

def process_datasets_page(offset, limit):
    """
    Fetch and process a single page of datasets from Hugging Face and cache them in Redis.
    """
    import redis
    import os
    import json
    import asyncio
    log = logging.getLogger(__name__)
    log.info(f"[process_datasets_page] ENTRY: offset={offset}, limit={limit}")
    token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        log.error("[process_datasets_page] HUGGINGFACEHUB_API_TOKEN environment variable is not set.")
        raise RuntimeError("HUGGINGFACEHUB_API_TOKEN environment variable is not set. Please set it securely.")
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (compatible; CollinearTool/1.0; +https://yourdomain.com)"
    }
    params = {"limit": limit, "offset": offset, "full": "True"}
    redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
    stream_key = "hf:datasets:all:stream"
    zset_key = "hf:datasets:all:zset"
    hash_key = "hf:datasets:all:hash"
    try:
        log.info(f"[process_datasets_page] Requesting {HF_API_URL} with params={params}")
        response = requests.get(HF_API_URL, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        
        page_items = response.json() 
        
        log.info(f"[process_datasets_page] Received {len(page_items)} datasets at offset {offset}")
        
        dataset_ids = [ds.get("id") for ds in page_items]
        size_map = asyncio.run(_fetch_sizes(dataset_ids))
        
        for ds in page_items:
            dataset_id = ds.get("id")
            size_bytes = size_map.get(dataset_id)
            downloads = ds.get("downloads")
            likes = ds.get("likes")
            impact_level, assessment_method = determine_impact_level_by_criteria(size_bytes, downloads, likes)
            metrics = DatasetMetrics(size_bytes=size_bytes, downloads=downloads, likes=likes)
            thresholds = {
                "size_bytes": {
                    "low": str(100 * 1024 * 1024),
                    "medium": str(1 * 1024 * 1024 * 1024),
                    "high": str(10 * 1024 * 1024 * 1024)
                }
            }
            impact_assessment = ImpactAssessment(
                dataset_id=dataset_id,
                impact_level=impact_level,
                assessment_method=assessment_method,
                metrics=metrics,
                thresholds=thresholds
            ).model_dump()
            item = {
                "id": dataset_id,
                "name": ds.get("name"),
                "description": ds.get("description"),
                "size_bytes": size_bytes,
                "impact_level": impact_level.value if isinstance(impact_level, ImpactLevel) else impact_level,
                "downloads": downloads,
                "likes": likes,
                "tags": ds.get("tags", []),
                "impact_assessment": json.dumps(impact_assessment)
            }
            final_item = {}
            for k, v in item.items():
                if isinstance(v, list) or isinstance(v, dict):
                     final_item[k] = json.dumps(v)
                elif v is None:
                    final_item[k] = 'null'
                else:
                    final_item[k] = str(v)

            redis_client.xadd(stream_key, final_item)
            redis_client.zadd(zset_key, {dataset_id: offset})
            redis_client.hset(hash_key, dataset_id, json.dumps(item))
            
        log.info(f"[process_datasets_page] EXIT: Cached {len(page_items)} datasets at offset {offset}")
        return len(page_items)
    except Exception as exc:
        log.error(f"[process_datasets_page] ERROR: offset={offset}, limit={limit}, exc={exc}", exc_info=True)
        raise

def refresh_datasets_cache():
    """
    Orchestrator: Enqueue Celery tasks to fetch all Hugging Face datasets in parallel.
    Uses direct calls to HF API.
    """
    import requests
    log.info("[refresh_datasets_cache] Orchestrating dataset fetch tasks using direct HF API calls.")
    token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        log.error("[refresh_datasets_cache] HUGGINGFACEHUB_API_TOKEN environment variable is not set.")
        raise RuntimeError("HUGGINGFACEHUB_API_TOKEN environment variable is not set. Please set it securely.")
        
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (compatible; CollinearTool/1.0; +https://yourdomain.com)"
    }
    limit = 500
    
    params = {"limit": 1, "offset": 0}
    try:
        response = requests.get(HF_API_URL, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        total_str = response.headers.get('X-Total-Count')
        if not total_str:
            log.error("[refresh_datasets_cache] 'X-Total-Count' header not found in HF API response.")
            raise ValueError("'X-Total-Count' header missing from Hugging Face API response.")
        total = int(total_str)
        log.info(f"[refresh_datasets_cache] Total datasets reported by HF API: {total}")
    except requests.RequestException as e:
        log.error(f"[refresh_datasets_cache] Error fetching total dataset count from HF API: {e}")
        raise
    except ValueError as e:
        log.error(f"[refresh_datasets_cache] Error parsing total dataset count: {e}")
        raise

    num_pages = (total + limit - 1) // limit
    from app.tasks.dataset_tasks import fetch_datasets_page
    from celery import group
    tasks = []
    for page_num in range(num_pages):
        offset = page_num * limit
        tasks.append(fetch_datasets_page.s(offset, limit))
        log.info(f"[refresh_datasets_cache] Scheduled page at offset {offset}, limit {limit}.")
    if tasks:
        group(tasks).apply_async()
        log.info(f"[refresh_datasets_cache] Enqueued {len(tasks)} fetch tasks.")
    else:
        log.warning("[refresh_datasets_cache] No dataset pages found to schedule.")

def determine_impact_level_by_criteria(size_bytes, downloads=None, likes=None):
    try:
        size = int(size_bytes) if size_bytes not in (None, 'null') else 0
    except Exception:
        size = 0

    # Prefer size_bytes if available
    if size >= 10 * 1024 * 1024 * 1024:
        return ("high", "large_size")
    elif size >= 1 * 1024 * 1024 * 1024:
        return ("medium", "medium_size")
    elif size >= 100 * 1024 * 1024:
        return ("low", "small_size")
    # Fallback to downloads if size_bytes is missing or too small
    if downloads is not None:
        try:
            downloads = int(downloads)
            if downloads >= 100000:
                return ("high", "downloads")
            elif downloads >= 10000:
                return ("medium", "downloads")
            elif downloads >= 1000:
                return ("low", "downloads")
        except Exception:
            pass
    # Fallback to likes if downloads is missing
    if likes is not None:
        try:
            likes = int(likes)
            if likes >= 1000:
                return ("high", "likes")
            elif likes >= 100:
                return ("medium", "likes")
            elif likes >= 10:
                return ("low", "likes")
        except Exception:
            pass
    return ("not_available", "size_and_downloads_and_likes_unknown")

def get_dataset_size(dataset: dict, dataset_id: str = None):
    """
    Extract the size in bytes from a dataset dictionary.
    Tries multiple locations based on possible HuggingFace API responses.
    """
    # Try top-level key
    size_bytes = dataset.get("size_bytes")
    if size_bytes not in (None, 'null'):
        return size_bytes
    # Try nested structure from the size API
    size_bytes = (
        dataset.get("size", {})
        .get("dataset", {})
        .get("num_bytes_original_files")
    )
    if size_bytes not in (None, 'null'):
        return size_bytes
    # Try metrics or info sub-dictionaries if present
    for key in ["metrics", "info"]:
        sub = dataset.get(key, {})
        if isinstance(sub, dict):
            size_bytes = sub.get("size_bytes")
            if size_bytes not in (None, 'null'):
                return size_bytes
    # Not found
    return None

async def get_datasets_page_from_zset_async(offset: int = 0, limit: int = 10, search: str = None) -> dict:
    redis_client = aioredis.Redis(host="redis", port=6379, db=0, decode_responses=True)
    zset_key = "hf:datasets:all:zset"
    hash_key = "hf:datasets:all:hash"
    total = await redis_client.zcard(zset_key)
    ids = await redis_client.zrange(zset_key, offset, offset + limit - 1)
    if not ids:
        return {"items": [], "count": total}
    items = await redis_client.hmget(hash_key, ids)
    parsed = []
    for raw in items:
        if not raw:
            continue
        try:
            item = json.loads(raw)
            parsed.append(item)
        except Exception:
            continue
    if search:
        parsed = [d for d in parsed if search.lower() in (d.get("id") or "").lower()]
    return {"items": parsed, "count": total}

async def get_dataset_commits_async(dataset_id: str, limit: int = 20):
    from huggingface_hub import HfApi
    import logging
    log = logging.getLogger(__name__)
    api = HfApi()
    log.info(f"[get_dataset_commits_async] Fetching commits for dataset_id={dataset_id}")
    try:
        # huggingface_hub is sync, so run in threadpool
        import anyio
        commits = await anyio.to_thread.run_sync(api.list_repo_commits, repo_id=dataset_id, repo_type="dataset")
        log.info(f"[get_dataset_commits_async] Received {len(commits)} commits for {dataset_id}")
    except Exception as e:
        log.error(f"[get_dataset_commits_async] Error fetching commits for {dataset_id}: {e}", exc_info=True)
        raise
    result = []
    for c in commits[:limit]:
        try:
            commit_id = getattr(c, "commit_id", "")
            title = getattr(c, "title", "")
            message = getattr(c, "message", title)
            authors = getattr(c, "authors", [])
            author_name = authors[0] if authors and isinstance(authors, list) else ""
            created_at = getattr(c, "created_at", None)
            if created_at:
                if hasattr(created_at, "isoformat"):
                    date = created_at.isoformat()
                else:
                    date = str(created_at)
            else:
                date = ""
            result.append({
                "id": commit_id or "",
                "title": title or message or "",
                "message": message or title or "",
                "author": {"name": author_name, "email": ""},
                "date": date,
            })
        except Exception as e:
            log.error(f"[get_dataset_commits_async] Error parsing commit: {e} | Commit: {getattr(c, '__dict__', str(c))}", exc_info=True)
    log.info(f"[get_dataset_commits_async] Returning {len(result)} parsed commits for {dataset_id}")
    return result

async def get_dataset_files_async(dataset_id: str) -> List[str]:
    from huggingface_hub import HfApi
    import anyio
    api = HfApi()
    # huggingface_hub is sync, so run in threadpool
    return await anyio.to_thread.run_sync(api.list_repo_files, repo_id=dataset_id, repo_type="dataset")

async def get_file_url_async(dataset_id: str, filename: str, revision: Optional[str] = None) -> str:
    from huggingface_hub import hf_hub_url
    import anyio
    # huggingface_hub is sync, so run in threadpool
    return await anyio.to_thread.run_sync(hf_hub_url, repo_id=dataset_id, filename=filename, repo_type="dataset", revision=revision)

# Fetch and cache all datasets

class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

async def fetch_size(session, dataset_id, token=None):
    url = f"https://datasets-server.huggingface.co/size?dataset={dataset_id}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        resp = await session.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return dataset_id, data.get("size", {}).get("dataset", {}).get("num_bytes_original_files")
    except Exception as e:
        log.warning(f"Could not fetch size for {dataset_id}: {e}")
    return dataset_id, None

async def fetch_all_sizes(dataset_ids, token=None, batch_size=50):
    results = {}
    async with httpx.AsyncClient() as session:
        for i in range(0, len(dataset_ids), batch_size):
            batch = dataset_ids[i:i+batch_size]
            tasks = [fetch_size(session, ds_id, token) for ds_id in batch]
            batch_results = await asyncio.gather(*tasks)
            for ds_id, size in batch_results:
                results[ds_id] = size
    return results

def fetch_and_cache_all_datasets(token: str):
    api = HfApi(token=token)
    log.info("Fetching all datasets from Hugging Face Hub...")
    all_datasets = list(api.list_datasets())
    all_datasets_dicts = []
    dataset_ids = [d.id for d in all_datasets]
    # Fetch all sizes in batches
    sizes = asyncio.run(fetch_all_sizes(dataset_ids, token=token, batch_size=50))
    for d in all_datasets:
        data = d.__dict__
        size_bytes = sizes.get(d.id)
        downloads = data.get("downloads")
        likes = data.get("likes")
        data["size_bytes"] = size_bytes
        impact_level, _ = determine_impact_level_by_criteria(size_bytes, downloads, likes)
        data["impact_level"] = impact_level
        all_datasets_dicts.append(data)
    compressed = gzip.compress(json.dumps(all_datasets_dicts, cls=EnhancedJSONEncoder).encode("utf-8"))
    r = redis.Redis(host="redis", port=6379, decode_responses=False)
    r.set(REDIS_KEY, compressed)
    log.info(f"Cached {len(all_datasets_dicts)} datasets in Redis under {REDIS_KEY}")
    return len(all_datasets_dicts)

# Native pagination from cache

def get_datasets_page_from_cache(limit: int, offset: int):
    r = redis.Redis(host="redis", port=6379, decode_responses=False)
    compressed = r.get(REDIS_KEY)
    if not compressed:
        return {"error": "Cache not found. Please refresh datasets."}, 404
    all_datasets = json.loads(gzip.decompress(compressed).decode("utf-8"))
    total = len(all_datasets)
    if offset < 0 or offset >= total:
        return {"error": "Offset out of range.", "total": total}, 400
    page = all_datasets[offset:offset+limit]
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
    }, 200