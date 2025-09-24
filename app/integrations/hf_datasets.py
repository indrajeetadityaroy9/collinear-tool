"""Utilities for interacting with Hugging Face datasets and caching metadata in Redis."""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
import requests
from huggingface_hub import HfApi
from redis import Redis

from app.core.config import settings
from app.integrations.redis_client import get_redis_sync
from app.schemas.dataset import ImpactAssessment
from app.schemas.dataset_common import DatasetMetrics

log = logging.getLogger(__name__)
HF_API_URL = "https://huggingface.co/api/datasets"
REDIS_BLOB_KEY = "hf:datasets:all:compressed"
REDIS_STREAM_KEY = "hf:datasets:all:stream"
REDIS_ZSET_KEY = "hf:datasets:all:zset"
REDIS_HASH_KEY = "hf:datasets:all:hash"
REDIS_META_KEY = "hf:datasets:meta"


class EnhancedJSONEncoder(json.JSONEncoder):
    """JSON encoder that understands datetime objects."""

    def default(self, obj: Any):  # type: ignore[override]
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _resolve_hf_token(explicit_token: Optional[str] = None) -> str:
    token = settings.resolve_hf_token(explicit_token)
    if not token:
        raise RuntimeError("Hugging Face API token is not configured.")
    return token


def _safe_get_redis() -> Optional[Redis]:
    try:
        return get_redis_sync()
    except Exception as exc:  # pragma: no cover - logged for observability
        log.warning("Redis unavailable: %s", exc)
        return None


def _stringify_for_stream(item: Dict[str, Any]) -> Dict[str, str]:
    payload: Dict[str, str] = {}
    for key, value in item.items():
        if value is None:
            payload[key] = "null"
        elif isinstance(value, (dict, list)):
            payload[key] = json.dumps(value, cls=EnhancedJSONEncoder)
        else:
            payload[key] = str(value)
    return payload


def _update_cache_meta(redis_client: Redis, total_items: int, *, refreshing: bool) -> None:
    meta = {
        "last_update": datetime.now(timezone.utc).isoformat(),
        "total_items": total_items,
        "refreshing": refreshing,
    }
    redis_client.set(REDIS_META_KEY, json.dumps(meta))


def _normalize_cached_item(item: Dict[str, Any]) -> Dict[str, Any]:
    assessment = item.get("impact_assessment")
    if isinstance(assessment, str):
        try:
            item["impact_assessment"] = json.loads(assessment)
        except json.JSONDecodeError:
            pass

    tags = item.get("tags")
    if isinstance(tags, str):
        try:
            item["tags"] = json.loads(tags)
        except json.JSONDecodeError:
            item["tags"] = [tags] if tags else []

    for numeric_field in ("size_bytes", "downloads", "likes"):
        value = item.get(numeric_field)
        if isinstance(value, str):
            if value.lower() == "null":
                item[numeric_field] = None
                continue
            try:
                item[numeric_field] = int(float(value))
            except (ValueError, TypeError):
                item[numeric_field] = None
    return item


def _load_all_cached_items(redis_client: Redis) -> List[Dict[str, Any]]:
    raw_items = redis_client.hgetall(REDIS_HASH_KEY)
    if raw_items:
        return [
            _normalize_cached_item(json.loads(value))
            for value in raw_items.values()
        ]

    compressed = redis_client.get(REDIS_BLOB_KEY)
    if compressed:
        try:
            if isinstance(compressed, str):
                compressed_bytes = base64.b64decode(compressed.encode("ascii"))
            else:
                compressed_bytes = compressed
            data = json.loads(gzip.decompress(compressed_bytes).decode("utf-8"))
            return [_normalize_cached_item(item) for item in data]
        except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
            log.error("Failed to decompress cached datasets: %s", exc)
    return []


def determine_impact_level_by_criteria(size_bytes, downloads=None, likes=None) -> Tuple[str, str]:
    """Classify dataset impact based on size, downloads, or likes."""
    try:
        size = int(size_bytes) if size_bytes not in (None, "null") else 0
    except Exception:
        size = 0

    if size >= 10 * 1024 * 1024 * 1024:
        return "high", "large_size"
    if size >= 1 * 1024 * 1024 * 1024:
        return "medium", "medium_size"
    if size >= 100 * 1024 * 1024:
        return "low", "small_size"

    if downloads is not None:
        try:
            downloads = int(downloads)
            if downloads >= 100_000:
                return "high", "downloads"
            if downloads >= 10_000:
                return "medium", "downloads"
            if downloads >= 1_000:
                return "low", "downloads"
        except Exception:
            pass

    if likes is not None:
        try:
            likes = int(likes)
            if likes >= 1_000:
                return "high", "likes"
            if likes >= 100:
                return "medium", "likes"
            if likes >= 10:
                return "low", "likes"
        except Exception:
            pass

    return "not_available", "size_and_downloads_and_likes_unknown"


async def _fetch_size(session: httpx.AsyncClient, dataset_id: str, token: Optional[str] = None) -> Optional[int]:
    url = f"https://datasets-server.huggingface.co/size?dataset={dataset_id}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        resp = await session.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("size", {}).get("dataset", {}).get("num_bytes_original_files")
    except Exception as exc:
        log.warning("Could not fetch size for %s: %s", dataset_id, exc)
    return None


async def _fetch_sizes(dataset_ids: Iterable[str], token: Optional[str] = None) -> Dict[str, Optional[int]]:
    results: Dict[str, Optional[int]] = {}
    dataset_ids = [dataset_id for dataset_id in dataset_ids if dataset_id]
    if not dataset_ids:
        return results

    async with httpx.AsyncClient() as session:
        tasks = [asyncio.create_task(_fetch_size(session, ds_id, token)) for ds_id in dataset_ids]
        for dataset_id, task in zip(dataset_ids, tasks):
            results[dataset_id] = await task
    return results


def _build_cached_item(dataset_id: str, raw: Dict[str, Any], size_bytes: Optional[int], downloads: Optional[int], likes: Optional[int]) -> Dict[str, Any]:
    impact_level, assessment_method = determine_impact_level_by_criteria(size_bytes, downloads, likes)
    metrics = DatasetMetrics(size_bytes=size_bytes, downloads=downloads, likes=likes)
    thresholds = {
        "size_bytes": {
            "low": str(100 * 1024 * 1024),
            "medium": str(1 * 1024 * 1024 * 1024),
            "high": str(10 * 1024 * 1024 * 1024),
        }
    }

    impact_assessment = ImpactAssessment(
        dataset_id=dataset_id,
        impact_level=impact_level,
        assessment_method=assessment_method,
        metrics=metrics,
        thresholds=thresholds,
    ).model_dump()

    item: Dict[str, Any] = {
        "id": dataset_id,
        "name": raw.get("name") or dataset_id,
        "description": raw.get("description"),
        "size_bytes": size_bytes,
        "impact_level": impact_level,
        "downloads": downloads,
        "likes": likes,
        "tags": raw.get("tags") or [],
        "card_data": raw.get("cardData"),
        "impact_assessment": impact_assessment,
    }

    for key, value in raw.items():  # include additional metadata for clients that expect it
        item.setdefault(key, value)

    return item


def process_datasets_page(offset: int, limit: int) -> int:
    """Fetch a page of datasets from Hugging Face and cache enriched metadata."""
    log.info("[process_datasets_page] ENTRY: offset=%s limit=%s", offset, limit)
    token = _resolve_hf_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (compatible; CollinearTool/1.0; +https://yourdomain.com)",
    }
    params = {"limit": limit, "offset": offset, "full": "True"}

    response = requests.get(HF_API_URL, headers=headers, params=params, timeout=120)
    response.raise_for_status()
    page_items = response.json()
    log.info("[process_datasets_page] Received %s datasets at offset %s", len(page_items), offset)

    dataset_ids = [item.get("id") for item in page_items]
    size_map = asyncio.run(_fetch_sizes(dataset_ids, token=token)) if dataset_ids else {}

    redis_client = _safe_get_redis()
    if redis_client is None:
        raise RuntimeError("Redis connection unavailable while processing datasets page")

    pipe = redis_client.pipeline()
    stored = 0
    for index, dataset in enumerate(page_items):
        dataset_id = dataset.get("id")
        if not dataset_id:
            continue
        size_bytes = size_map.get(dataset_id)
        downloads = dataset.get("downloads")
        likes = dataset.get("likes")
        cached_item = _build_cached_item(dataset_id, dataset, size_bytes, downloads, likes)
        pipe.xadd(REDIS_STREAM_KEY, _stringify_for_stream(cached_item))
        pipe.zadd(REDIS_ZSET_KEY, {dataset_id: offset + index})
        pipe.hset(REDIS_HASH_KEY, dataset_id, json.dumps(cached_item, cls=EnhancedJSONEncoder))
        stored += 1

    pipe.execute()
    total_items = redis_client.zcard(REDIS_ZSET_KEY)
    _update_cache_meta(redis_client, total_items, refreshing=False)

    if stored:
        redis_client.delete(REDIS_BLOB_KEY)  # mark bulk snapshot stale until next full refresh

    log.info("[process_datasets_page] EXIT: Cached %s datasets at offset %s", stored, offset)
    return stored


def refresh_datasets_cache() -> None:
    """Schedule Celery tasks to refresh cached dataset metadata."""
    log.info("[refresh_datasets_cache] Orchestrating dataset fetch tasks using direct HF API calls.")
    token = _resolve_hf_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (compatible; CollinearTool/1.0; +https://yourdomain.com)",
    }

    response = requests.get(HF_API_URL, headers=headers, params={"limit": 1, "offset": 0}, timeout=120)
    response.raise_for_status()

    total_str = response.headers.get("X-Total-Count")
    if not total_str:
        raise ValueError("'X-Total-Count' header missing from Hugging Face API response.")

    total = int(total_str)
    limit = 500
    num_pages = (total + limit - 1) // limit

    redis_client = _safe_get_redis()
    if redis_client is not None:
        current_total = redis_client.zcard(REDIS_ZSET_KEY)
        _update_cache_meta(redis_client, current_total, refreshing=True)

    from app.tasks.dataset_tasks import fetch_datasets_page
    from celery import group

    tasks = [fetch_datasets_page.s(page * limit, limit) for page in range(num_pages)]
    if tasks:
        group(tasks).apply_async()
        log.info("[refresh_datasets_cache] Enqueued %s fetch tasks.", len(tasks))


async def get_dataset_commits_async(dataset_id: str, limit: int = 20):
    """Return commit history for a dataset via the Hugging Face Hub SDK."""
    api = HfApi()
    log.info("[get_dataset_commits_async] Fetching commits for dataset_id=%s", dataset_id)
    try:
        import anyio

        commits = await anyio.to_thread.run_sync(api.list_repo_commits, repo_id=dataset_id, repo_type="dataset")
    except Exception as exc:
        log.error("[get_dataset_commits_async] Error fetching commits for %s: %s", dataset_id, exc, exc_info=True)
        raise

    result = []
    for commit in commits[:limit]:
        try:
            commit_id = getattr(commit, "commit_id", "")
            title = getattr(commit, "title", "")
            message = getattr(commit, "message", title)
            authors = getattr(commit, "authors", [])
            author_name = authors[0] if authors and isinstance(authors, list) else ""
            created_at = getattr(commit, "created_at", None)
            date = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or "")
            result.append(
                {
                    "id": commit_id or "",
                    "title": title or message or "",
                    "message": message or title or "",
                    "author": {"name": author_name, "email": ""},
                    "date": date,
                }
            )
        except Exception as exc:
            log.error("[get_dataset_commits_async] Error parsing commit: %s", exc, exc_info=True)
    return result


async def get_dataset_files_async(dataset_id: str) -> List[str]:
    """Return a list of files for a Hugging Face dataset."""
    api = HfApi()
    import anyio

    return await anyio.to_thread.run_sync(api.list_repo_files, repo_id=dataset_id, repo_type="dataset")


async def get_file_url_async(dataset_id: str, filename: str, revision: Optional[str] = None) -> str:
    """Return a download URL for a dataset file."""
    from huggingface_hub import hf_hub_url
    import anyio

    return await anyio.to_thread.run_sync(
        hf_hub_url,
        repo_id=dataset_id,
        filename=filename,
        repo_type="dataset",
        revision=revision,
    )


async def fetch_size(session: httpx.AsyncClient, dataset_id, token=None):
    return await _fetch_size(session, dataset_id, token)


async def fetch_all_sizes(dataset_ids, token=None, batch_size=50):
    results: Dict[str, Optional[int]] = {}
    dataset_ids = list(dataset_ids)
    if not dataset_ids:
        return results

    async with httpx.AsyncClient() as session:
        for i in range(0, len(dataset_ids), batch_size):
            batch = dataset_ids[i : i + batch_size]
            tasks = [fetch_size(session, ds_id, token) for ds_id in batch]
            batch_results = await asyncio.gather(*tasks)
            for ds_id, size in batch_results:
                results[ds_id] = size
    return results


def fetch_and_cache_all_datasets(token: Optional[str] = None):
    resolved_token = _resolve_hf_token(token)
    api = HfApi(token=resolved_token)
    log.info("Fetching all datasets from Hugging Face Hub...")
    all_datasets = list(api.list_datasets())
    dataset_ids = [dataset.id for dataset in all_datasets]
    sizes = asyncio.run(fetch_all_sizes(dataset_ids, token=resolved_token, batch_size=50)) if dataset_ids else {}

    redis_client = _safe_get_redis()
    if redis_client is None:
        raise RuntimeError("Redis connection unavailable while caching datasets")

    payload: List[Dict[str, Any]] = []
    pipe = redis_client.pipeline()
    pipe.delete(REDIS_STREAM_KEY, REDIS_ZSET_KEY, REDIS_HASH_KEY, REDIS_BLOB_KEY)

    for index, dataset in enumerate(all_datasets):
        raw_data = json.loads(json.dumps(dataset.__dict__, cls=EnhancedJSONEncoder))
        dataset_id = dataset.id
        size_bytes = sizes.get(dataset_id)
        downloads = raw_data.get("downloads")
        likes = raw_data.get("likes")
        cached_item = _build_cached_item(dataset_id, raw_data, size_bytes, downloads, likes)
        payload.append(cached_item)
        pipe.xadd(REDIS_STREAM_KEY, _stringify_for_stream(cached_item))
        pipe.zadd(REDIS_ZSET_KEY, {dataset_id: index})
        pipe.hset(REDIS_HASH_KEY, dataset_id, json.dumps(cached_item, cls=EnhancedJSONEncoder))

    compressed = gzip.compress(json.dumps(payload, cls=EnhancedJSONEncoder).encode("utf-8")) if payload else b""
    if compressed:
        encoded = base64.b64encode(compressed).decode("ascii")
        pipe.set(REDIS_BLOB_KEY, encoded)

    pipe.execute()
    _update_cache_meta(redis_client, len(payload), refreshing=False)
    log.info("Cached %s datasets in Redis under %s", len(payload), REDIS_BLOB_KEY)
    return len(payload)


def _sort_value(value: Any) -> Tuple[int, Any]:
    if value is None:
        return (2, "")
    if isinstance(value, (int, float)):
        return (0, value)
    if isinstance(value, str):
        stripped = value.strip()
        try:
            numeric = float(stripped)
            return (0, numeric)
        except ValueError:
            return (1, stripped.lower())
    return (2, str(value).lower())


def _build_paginated_response(items: List[Dict[str, Any]], total: int, limit: int, offset: int) -> Dict[str, Any]:
    if total == 0:
        return {
            "total": 0,
            "current_page": 0,
            "total_pages": 0,
            "next_page": None,
            "prev_page": None,
            "items": [],
        }

    total_pages = (total + limit - 1) // limit if limit else 0
    if offset >= total:
        current_page = total_pages if total_pages else 0
        page_items: List[Dict[str, Any]] = []
    else:
        current_page = (offset // limit) + 1 if limit else 1
        page_items = items[offset : offset + limit]

    next_page = current_page + 1 if current_page < total_pages else None
    prev_page = current_page - 1 if current_page > 1 else None

    return {
        "total": total,
        "current_page": current_page,
        "total_pages": total_pages,
        "next_page": next_page,
        "prev_page": prev_page,
        "items": page_items,
    }


def get_datasets_page_from_cache(
    limit: int,
    offset: int,
    search: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_order: str = "desc",
):
    redis_client = _safe_get_redis()
    if redis_client is None:
        response = _build_paginated_response([], 0, limit, offset)
        response["warming_up"] = True
        return response, 200

    total = redis_client.zcard(REDIS_ZSET_KEY)
    needs_full_scan = bool(search or sort_by)
    all_items: Optional[List[Dict[str, Any]]] = None

    if total == 0 or needs_full_scan:
        all_items = _load_all_cached_items(redis_client)
        if total == 0:
            total = len(all_items)

    if total == 0:
        response = _build_paginated_response([], 0, limit, offset)
        response["warming_up"] = True
        return response, 200

    if needs_full_scan:
        assert all_items is not None
        items = all_items
        if search:
            lowered = search.lower()
            items = [
                item
                for item in items
                if lowered in ((item.get("id") or "").lower() + " " + str(item.get("description") or "").lower())
            ]
        if sort_by:
            reverse = sort_order != "asc"
            items.sort(key=lambda dataset: _sort_value(dataset.get(sort_by)), reverse=reverse)
        total_filtered = len(items)
        response = _build_paginated_response(items, total_filtered, limit, offset)
        response["warming_up"] = False
        return response, 200

    ids = redis_client.zrange(REDIS_ZSET_KEY, offset, offset + limit - 1)
    if ids:
        raw_items = redis_client.hmget(REDIS_HASH_KEY, ids)
        page_items = [
            _normalize_cached_item(json.loads(item))
            for item in raw_items
            if item
        ]
    else:
        page_items = []

    response = _build_paginated_response(page_items, total, limit, offset)
    response["warming_up"] = False
    return response, 200
