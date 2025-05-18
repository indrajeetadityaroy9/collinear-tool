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

log = logging.getLogger(__name__)
api = HfApi()

def _get_repo_size(dataset_id: str) -> int:
    """
    Return the total size of the dataset repository in **bytes**.
    """
    try:
        repo_info = api.repo_info(repo_id=dataset_id, repo_type="dataset")
        return repo_info.size 
    except RepositoryNotFoundError as exc:
        raise ValueError(f"Dataset '{dataset_id}' not found") from exc


async def get_repo_size_async(dataset_id: str) -> int:
    return await anyio.to_thread.run_sync(_get_repo_size, dataset_id)

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


async def list_datasets_async(
    limit: int | None = None,
    search: str | None = None,
) -> list[dict[str, Any]]:
    def _worker() -> list[dict[str, Any]]:
        infos = api.list_datasets(limit=limit, search=search)
        return [_dataset_info_to_dict(i) for i in infos]

    return await anyio.to_thread.run_sync(_worker)

def _commit_history(dataset_id: str) -> List[dict]:
    commits = list_repo_commits(
        repo_id=dataset_id,
        repo_type="dataset",
        formatted=False,
    )
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
        paths = api.list_repo_files(
            repo_id=dataset_id,
            repo_type="dataset",
        )
    except RepositoryNotFoundError as exc:
        raise ValueError(f"Dataset '{dataset_id}' not found") from exc

    filtered = _filter_dataset_files(paths)

    if not filtered:
        filtered = _fetch_parquet_paths_via_viewer(dataset_id)

    return filtered


async def list_repo_files_async(dataset_id: str) -> list[str]:
    return await anyio.to_thread.run_sync(_list_repo_files, dataset_id)

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


__all__ = [
    "list_datasets_async",
    "commit_history_async",
    "list_repo_files_async",
    "get_file_download_url_async",
     "get_repo_size_async"
]

