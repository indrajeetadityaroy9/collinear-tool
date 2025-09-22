"""Utilities for interacting with Hugging Face datasets and caching metadata in Redis."""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx
import redis
import requests
from huggingface_hub import HfApi

from app.schemas.dataset import ImpactAssessment
from app.schemas.dataset_common import DatasetMetrics

log = logging.getLogger(__name__)
HF_API_URL = "https://huggingface.co/api/datasets"
REDIS_KEY = "hf:datasets:all:compressed"


def _get_sync_redis() -> redis.Redis:
    """Return a synchronous Redis client using Docker-compose defaults."""
    return redis.Redis(host="redis", port=6379, decode_responses=True)


async def _fetch_size(session: httpx.AsyncClient, dataset_id: str) -> Optional[int]:
    """Fetch dataset size from the Hugging Face datasets server."""
    url = f"https://datasets-server.huggingface.co/size?dataset={dataset_id}"
    try:
        resp = await session.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("size", {}).get("dataset", {}).get("num_bytes_original_files")
    except Exception as exc:  # pragma: no cover - best-effort telemetry
        log.warning("Could not fetch size for %s: %s", dataset_id, exc)
    return None


async def _fetch_sizes(dataset_ids: List[str]) -> Dict[str, Optional[int]]:
    """Fetch dataset sizes concurrently for the supplied ids."""
    results: Dict[str, Optional[int]] = {}
    async with httpx.AsyncClient() as session:
        tasks = [asyncio.create_task(_fetch_size(session, ds_id)) for ds_id in dataset_ids]
        for dataset_id, task in zip(dataset_ids, tasks):
            results[dataset_id] = await task
    return results


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


def process_datasets_page(offset: int, limit: int) -> int:
    """Fetch a page of datasets from Hugging Face and cache enriched metadata."""
    log.info("[process_datasets_page] ENTRY: offset=%s limit=%s", offset, limit)

    token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        raise RuntimeError("HUGGINGFACEHUB_API_TOKEN environment variable is not set.")

    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (compatible; CollinearTool/1.0; +https://yourdomain.com)",
    }
    params = {"limit": limit, "offset": offset, "full": "True"}

    redis_client = _get_sync_redis()
    stream_key = "hf:datasets:all:stream"
    zset_key = "hf:datasets:all:zset"
    hash_key = "hf:datasets:all:hash"

    response = requests.get(HF_API_URL, headers=headers, params=params, timeout=120)
    response.raise_for_status()
    page_items = response.json()
    log.info("[process_datasets_page] Received %s datasets at offset %s", len(page_items), offset)

    dataset_ids = [ds.get("id") for ds in page_items]
    size_map = asyncio.run(_fetch_sizes(dataset_ids)) if dataset_ids else {}

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

        item = {
            "id": dataset_id,
            "name": ds.get("name"),
            "description": ds.get("description"),
            "size_bytes": size_bytes,
            "impact_level": impact_level,
            "downloads": downloads,
            "likes": likes,
            "tags": ds.get("tags", []),
            "impact_assessment": json.dumps(impact_assessment),
        }

        final_item = {}
        for key, value in item.items():
            if isinstance(value, (list, dict)):
                final_item[key] = json.dumps(value)
            elif value is None:
                final_item[key] = "null"
            else:
                final_item[key] = str(value)

        redis_client.xadd(stream_key, final_item)
        redis_client.zadd(zset_key, {dataset_id: offset})
        redis_client.hset(hash_key, dataset_id, json.dumps(item))

    log.info("[process_datasets_page] EXIT: Cached %s datasets at offset %s", len(page_items), offset)
    return len(page_items)


def refresh_datasets_cache() -> None:
    """Schedule Celery tasks to refresh cached dataset metadata."""
    log.info("[refresh_datasets_cache] Orchestrating dataset fetch tasks using direct HF API calls.")

    token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        raise RuntimeError("HUGGINGFACEHUB_API_TOKEN environment variable is not set.")

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
        except Exception as exc:  # pragma: no cover - defensive logging around vendor objects
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


class EnhancedJSONEncoder(json.JSONEncoder):
    """JSON encoder that understands datetime objects."""

    def default(self, obj):  # type: ignore[override]
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


async def fetch_size(session: httpx.AsyncClient, dataset_id, token=None):
    url = f"https://datasets-server.huggingface.co/size?dataset={dataset_id}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        resp = await session.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return dataset_id, data.get("size", {}).get("dataset", {}).get("num_bytes_original_files")
    except Exception as exc:  # pragma: no cover - telemetry only
        log.warning("Could not fetch size for %s: %s", dataset_id, exc)
    return dataset_id, None


async def fetch_all_sizes(dataset_ids, token=None, batch_size=50):
    results = {}
    async with httpx.AsyncClient() as session:
        for i in range(0, len(dataset_ids), batch_size):
            batch = dataset_ids[i : i + batch_size]
            tasks = [fetch_size(session, ds_id, token) for ds_id in batch]
            batch_results = await asyncio.gather(*tasks)
            for ds_id, size in batch_results:
                results[ds_id] = size
    return results


def fetch_and_cache_all_datasets(token: str):
    api = HfApi(token=token)
    log.info("Fetching all datasets from Hugging Face Hub...")
    all_datasets = list(api.list_datasets())
    dataset_ids = [dataset.id for dataset in all_datasets]
    sizes = asyncio.run(fetch_all_sizes(dataset_ids, token=token, batch_size=50)) if dataset_ids else {}

    payload = []
    for dataset in all_datasets:
        data = dataset.__dict__
        size_bytes = sizes.get(dataset.id)
        downloads = data.get("downloads")
        likes = data.get("likes")
        impact_level, _ = determine_impact_level_by_criteria(size_bytes, downloads, likes)
        data["size_bytes"] = size_bytes
        data["impact_level"] = impact_level
        payload.append(data)

    compressed = gzip.compress(json.dumps(payload, cls=EnhancedJSONEncoder).encode("utf-8"))
    redis_client = _get_sync_redis()
    redis_client.set(REDIS_KEY, compressed)
    log.info("Cached %s datasets in Redis under %s", len(payload), REDIS_KEY)
    return len(payload)


def get_datasets_page_from_cache(limit: int, offset: int):
    redis_client = _get_sync_redis()
    compressed = redis_client.get(REDIS_KEY)
    if not compressed:
        return {"error": "Cache not found. Please refresh datasets."}, 404

    all_datasets = json.loads(gzip.decompress(compressed).decode("utf-8"))
    total = len(all_datasets)
    if offset < 0 or offset >= total:
        return {"error": "Offset out of range.", "total": total}, 400

    page = all_datasets[offset : offset + limit]
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
        "items": page,
    }, 200
