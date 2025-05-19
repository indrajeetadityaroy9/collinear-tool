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
REDIS_KEY = "hf:datasets:all"
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
    return api.list_repo_files(dataset_id, repo_type="dataset")

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

def refresh_datasets_cache():
    """Fetch all datasets (200,000+) from HuggingFace and stream batches to Redis as a stream (XADD)."""
    import redis
    log.info("[refresh_datasets_cache] Fetching ALL datasets from HuggingFace via local proxy (streaming to Redis Stream).")
    try:
        token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
        if not token:
            raise RuntimeError("HUGGINGFACEHUB_API_TOKEN environment variable is not set. Please set it securely.")
        headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": "Mozilla/5.0 (compatible; CollinearTool/1.0; +https://yourdomain.com)"
        }
        proxy_url = "http://host.docker.internal:8080/api/datasets"
        page_size = 500
        offset = 0
        redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
        stream_key = "hf:datasets:all:stream"
        # Delete previous stream
        redis_client.delete(stream_key)
        total = 0
        while True:
            params = {"limit": page_size, "offset": offset, "full": "True"}
            for attempt in range(5):
                try:
                    response = requests.get(proxy_url, headers=headers, params=params, timeout=120)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    log.warning(f"[refresh_datasets_cache] Request failed (attempt {attempt+1}): {e}")
                    if attempt == 4:
                        raise
                    time.sleep(2 ** attempt)
            page = response.json()
            if not page:
                break
            for ds in page:
                dataset_id = ds.get("id")
                card_data = ds.get("cardData") or {}
                # Fetch accurate size_bytes from Datasets Server API
                size_bytes = None
                try:
                    size_url = f"https://datasets-server.huggingface.co/size?dataset={dataset_id}"
                    # Do NOT use Authorization header for this API
                    size_resp = requests.get(size_url, timeout=30)
                    log.info(f"[refresh_datasets_cache] Size API {dataset_id} status: {size_resp.status_code}")
                    if size_resp.ok:
                        size_data = size_resp.json()
                        log.info(f"[refresh_datasets_cache] Size API {dataset_id} response: {size_data}")
                        size_bytes = size_data.get("size", {}).get("dataset", {}).get("num_bytes_original_files")
                    else:
                        log.warning(f"[refresh_datasets_cache] Size API {dataset_id} failed: {size_resp.text}")
                except Exception as e:
                    log.warning(f"[refresh_datasets_cache] Could not fetch size for {dataset_id}: {e}")
                time.sleep(0.1)  # avoid rate limiting
                downloads = ds.get("downloads")
                likes = ds.get("likes")
                # Always compute impact_level from criteria
                impact_level, _ = determine_impact_level_by_criteria(size_bytes, downloads, likes)
                item = {
                    "id": dataset_id,
                    "name": ds.get("name"),
                    "description": ds.get("description"),
                    "size_bytes": size_bytes,
                    "impact_level": impact_level,
                    "downloads": downloads,
                    "likes": likes,
                    "tags": json.dumps(ds.get("tags", [])),
                    "impact_assessment": json.dumps(ds.get("impact_assessment", {}))
                }
                # Ensure all values are str, int, float, or bytes, and convert None to 'null'
                for k, v in item.items():
                    if v is None:
                        item[k] = 'null'
                    elif not isinstance(v, (str, int, float, bytes)):
                        item[k] = json.dumps(v)
                redis_client.xadd(stream_key, item)
                # Hybrid: also upsert into Sorted Set and Hash
                zset_key = "hf:datasets:all:zset"
                hash_key = "hf:datasets:all:hash"
                redis_client.zadd(zset_key, {dataset_id: offset + total})
                redis_client.hset(hash_key, dataset_id, json.dumps(item))
                total += 1
            log.info(f"[refresh_datasets_cache] Streamed {len(page)} datasets (offset {offset}), total so far: {total}")
            if len(page) < page_size:
                break
            offset += page_size
        log.info(f"[refresh_datasets_cache] Total datasets streamed to Redis Stream: {total}")
        # Store meta info
        meta_key = "hf:datasets:meta"
        meta = {"last_update": datetime.now(timezone.utc).isoformat(), "total_items": total}
        redis_client.set(meta_key, json.dumps(meta))
        log.info(json.dumps({"event": "refresh_datasets_cache", "total_items": total, "timestamp": meta["last_update"]}))
    except Exception as e:
        log.error(f"[refresh_datasets_cache] Exception: {e}", exc_info=True)

def determine_impact_level_by_criteria(size_bytes, downloads=None, likes=None):
    """
    Determine the impact level of a dataset based only on size_bytes, using data-driven thresholds:
    - high: >= 10GB
    - medium: >= 1GB
    - low: >= 100MB
    - not_available: < 100MB or null
    Returns (impact_level, method).
    """
    try:
        size = int(size_bytes) if size_bytes not in (None, 'null') else 0
    except Exception:
        size = 0

    if size >= 10 * 1024 * 1024 * 1024:  # 10 GB
        return ("high", "large_size")
    elif size >= 1 * 1024 * 1024 * 1024:  # 1 GB
        return ("medium", "medium_size")
    elif size >= 100 * 1024 * 1024:       # 100 MB
        return ("low", "small_size")
    else:
        return ("not_available", "size_unknown")

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