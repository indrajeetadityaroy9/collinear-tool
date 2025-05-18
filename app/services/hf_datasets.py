import functools
import logging
from typing import Any, List, Optional, Sequence, Set

import anyio
import requests
from huggingface_hub import (
    DatasetInfo,
    HfApi,
    hf_hub_url,
    list_repo_commits,
)
from huggingface_hub.utils import RepositoryNotFoundError, RevisionNotFoundError
from huggingface_hub.errors import HfHubHTTPError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from fastapi import HTTPException
from app.core.config import settings

log = logging.getLogger(__name__)
api = HfApi()

def get_hf_token():
    token = settings.hf_api_token.get_secret_value() if settings.hf_api_token else None
    log.info(f"Using HuggingFace token: {token[:6]}..." if token else "No HuggingFace token set!")
    return token

# Fail fast on 429, no retries
retry_hf = retry(
    stop=stop_after_attempt(1),
    retry=retry_if_exception_type(HfHubHTTPError),
    reraise=True
)

def _dataset_info_to_dict(info: DatasetInfo) -> dict[str, Any]:
    """Keep only JSON-serialisable bits we care about."""
    return {
        "id": info.id,
        "cardData": info.cardData,          
        "lastModified": info.lastModified, 
        "likes": info.likes,
        "gated": info.gated,
        "sha": info.sha,
    }

def list_datasets_via_requests(limit=10, full=True):
    token = get_hf_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = {"limit": str(limit), "full": str(full)}
    response = requests.get("https://huggingface.co/api/datasets", params=params, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()

async def list_datasets_async(
    limit: int | None = None,
    search: str | None = None,
    include_size: bool = False,
) -> list[dict[str, Any]]:
    def _worker() -> list[dict[str, Any]]:
        # Use direct requests.get for HuggingFace API
        api_limit = limit if limit is not None else 10
        data = list_datasets_via_requests(limit=api_limit, full=True)
        # The returned data is a list of dataset dicts
        results = []
        for info in data:
            # Optionally filter/search here if needed
            results.append(info)
        return results
    return await anyio.to_thread.run_sync(_worker)

def _commit_history(dataset_id: str) -> List[dict]:
    commits = list_repo_commits_with_retry(dataset_id)
    return [c.__dict__ for c in commits]


async def commit_history_async(dataset_id: str) -> List[dict]:
    return await anyio.to_thread.run_sync(_commit_history, dataset_id)


_DATASET_SUFFIXES: Set[str] = {
    ".csv",
    ".csv.gz",
    ".json",
    ".json.gz",
    ".jsonl",
    ".jsonl.gz",
    ".parquet",
    ".arrow",
}

_PARQUET_ENDPOINT = (
    "https://datasets-server.huggingface.co/parquet?dataset={dataset_id}"
)


def _filter_dataset_files(paths: Sequence[str]) -> list[str]:
    allowed = tuple(_DATASET_SUFFIXES)
    return sorted(p for p in paths if p.lower().endswith(allowed))


def _fetch_parquet_paths_via_viewer(dataset_id: str) -> list[str]:
    url = _PARQUET_ENDPOINT.format(dataset_id=dataset_id)
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
    except Exception as exc:
        log.debug("Dataset-Viewer API failed for %s → %s", dataset_id, exc)
        return []

    data = r.json()
    parquet_files = data.get("parquet_files", [])
    paths = {pf.get("relative_path") or pf.get("file_path") for pf in parquet_files}
    return sorted(p for p in paths if p)


def _list_repo_files(dataset_id: str) -> list[str]:
    try:
        paths = list_repo_files_with_retry(dataset_id)
    except RepositoryNotFoundError as exc:
        raise ValueError(f"Dataset '{dataset_id}' not found") from exc

    filtered = _filter_dataset_files(paths)

    if not filtered:
        filtered = _fetch_parquet_paths_via_viewer(dataset_id)

    return filtered


@retry_hf
def list_repo_files_with_retry(dataset_id):
    token = get_hf_token()
    try:
        return api.list_repo_files(dataset_id, token=token, repo_type="dataset")
    except HfHubHTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            log.error(f"Rate limited by HuggingFace Hub. Token used: {token[:6]}...")
            raise HTTPException(
                status_code=503,
                detail="HuggingFace API rate limit reached. Please try again later."
            )
        log.error(f"HuggingFace Hub error: {e}")
        raise

async def list_repo_files_async(dataset_id):
    try:
        return await anyio.to_thread.run_sync(list_repo_files_with_retry, dataset_id)
    except HfHubHTTPError as e:
        log.error(f"Failed to fetch files for {dataset_id}: {e}")
        raise HTTPException(
            status_code=503,
            detail="HuggingFace API unavailable or rate limited. Please try again later."
        )

def _get_file_download_url(
    dataset_id: str,
    filename: str,
    revision: str | None,
) -> str:
    try:
        return hf_hub_url(
            repo_id=dataset_id,
            filename=filename,
            repo_type="dataset",
            revision=revision,
        )
    except (RepositoryNotFoundError, RevisionNotFoundError) as exc:
        raise ValueError(str(exc)) from exc


async def get_file_download_url_async(
    dataset_id: str,
    filename: str,
    *,
    revision: Optional[str] = None,
) -> str:
    fn = functools.partial(_get_file_download_url, dataset_id, filename, revision)
    return await anyio.to_thread.run_sync(fn)

@retry_hf
def list_repo_commits_with_retry(dataset_id, revision="main"):
    token = get_hf_token()
    try:
        return api.list_repo_commits(dataset_id, revision=revision, token=token, repo_type="dataset")
    except HfHubHTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            log.error(f"Rate limited by HuggingFace Hub. Token used: {token[:6]}...")
            raise HTTPException(
                status_code=503,
                detail="HuggingFace API rate limit reached. Please try again later."
            )
        log.error(f"HuggingFace Hub error: {e}")
        raise

async def list_repo_commits_async(dataset_id, revision="main"):
    try:
        return await anyio.to_thread.run_sync(list_repo_commits_with_retry, dataset_id, revision)
    except HfHubHTTPError as e:
        log.error(f"Failed to fetch commits for {dataset_id}: {e}")
        raise HTTPException(
            status_code=503,
            detail="HuggingFace API unavailable or rate limited. Please try again later."
        )

__all__ = [
    "list_datasets_async",
    "commit_history_async",
    "list_repo_files_async",
    "get_file_download_url_async"
]

