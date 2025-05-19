import functools
import logging
import json
from typing import Any, List, Optional, Sequence, Set, Dict, Tuple

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
from app.schemas.dataset_common import ImpactLevel, DatasetMetrics
from app.services.redis_client import cache_get, cache_set, generate_cache_key
from app.services.dataset_impacts import get_stored_dataset_impact

log = logging.getLogger(__name__)
api = HfApi()

# Define size thresholds for impact categorization (in bytes)
# These thresholds are based on dataset size:
# - Low impact: < 100 MB
# - Medium impact: 100 MB - 1 GB
# - High impact: > 1 GB
SIZE_THRESHOLD_LOW = 100 * 1024 * 1024  # 100 MB
SIZE_THRESHOLD_MEDIUM = 1024 * 1024 * 1024  # 1 GB

# Define popularity thresholds
DOWNLOADS_THRESHOLD_LOW = 1000  # Low: < 1000 downloads
DOWNLOADS_THRESHOLD_MEDIUM = 10000  # Medium: 1000-10000 downloads, High: > 10000

LIKES_THRESHOLD_LOW = 10  # Low: < 10 likes
LIKES_THRESHOLD_MEDIUM = 100  # Medium: 10-100 likes, High: > 100

# Dataset viewer API endpoint
DATASET_INFO_ENDPOINT = "https://datasets-server.huggingface.co/info?dataset={dataset_id}"
# Direct dataset endpoint to get metrics
DATASET_METRICS_ENDPOINT = "https://huggingface.co/api/datasets/{dataset_id}"

# Constants for dataset API access and caching
HF_API_URL = "https://huggingface.co/api/datasets"
DATASET_CACHE_TTL = 60 * 60  # Cache datasets list for 1 hour

def get_hf_token():
    token = settings.HF_API_TOKEN.get_secret_value() if hasattr(settings, 'HF_API_TOKEN') and settings.HF_API_TOKEN else None
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
        # Other key metrics
        "downloads": getattr(info, "downloads", None),
        "downloadsAllTime": getattr(info, "downloads_all_time", None),
    }

def list_datasets_via_requests(limit=10, full=True):
    token = get_hf_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = {"limit": str(limit), "full": str(full)}
    response = requests.get("https://huggingface.co/api/datasets", params=params, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()

def fetch_dataset_viewer_info(dataset_id: str) -> Dict[str, Any]:
    """
    Fetch detailed dataset information from the HF dataset-viewer API.
    
    Returns:
        Dictionary containing dataset info including size, splits, etc.
    """
    url = DATASET_INFO_ENDPOINT.format(dataset_id=dataset_id)
    token = get_hf_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get("dataset_info", {})
    except Exception as exc:
        log.debug(f"Dataset-Viewer API failed for {dataset_id} → {exc}")
        return {}

def fetch_dataset_metrics(dataset_id: str) -> Dict[str, Any]:
    """
    Fetch dataset metrics directly from the HuggingFace API.
    
    Returns:
        Dictionary containing metrics including downloads, likes, etc.
    """
    url = DATASET_METRICS_ENDPOINT.format(dataset_id=dataset_id)
    token = get_hf_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    
    try:
        log.info(f"Fetching metrics for {dataset_id} from {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        log.info(f"Received metrics for {dataset_id}: downloads={data.get('downloads', None)}, likes={data.get('likes', None)}")
        return data
    except Exception as exc:
        log.warning(f"Failed to fetch metrics for {dataset_id}: {exc}")
        return {}

def determine_impact_level_by_criteria(size: int | None, downloads: int | None, likes: int | None) -> Tuple[ImpactLevel, str]:
    """
    Determine the impact level based on multiple criteria.
    
    Args:
        size: Size of the dataset in bytes
        downloads: Number of downloads (all time)
        likes: Number of likes
        
    Returns:
        Tuple of (impact_level, assessment_method)
    """
    # Default scores if metrics are missing
    size_score = 1  # Medium by default
    popularity_score = 1  # Medium by default
    
    # Calculate size impact (0 = low, 1 = medium, 2 = high)
    if size is not None:
        if size < SIZE_THRESHOLD_LOW:
            size_score = 0
        elif size < SIZE_THRESHOLD_MEDIUM:
            size_score = 1
        else:
            size_score = 2
    
    # Calculate popularity impact using downloads and likes
    download_score = 1
    if downloads is not None:
        if downloads < DOWNLOADS_THRESHOLD_LOW:
            download_score = 0
        elif downloads < DOWNLOADS_THRESHOLD_MEDIUM:
            download_score = 1
        else:
            download_score = 2
    
    like_score = 1
    if likes is not None:
        if likes < LIKES_THRESHOLD_LOW:
            like_score = 0
        elif likes < LIKES_THRESHOLD_MEDIUM:
            like_score = 1
        else:
            like_score = 2
    
    # Combine popularity metrics (max of downloads and likes)
    popularity_score = max(download_score, like_score)
    
    # Calculate overall impact (average of size and popularity, rounded up)
    overall_score = (size_score + popularity_score) / 2
    
    # Determine assessment method based on which metrics were available
    methods = []
    if size is not None:
        methods.append("size")
    if downloads is not None:
        methods.append("downloads")
    if likes is not None:
        methods.append("likes")
    
    assessment_method = "_and_".join(methods) + "_based" if methods else "unknown"
    
    # Map score to impact level
    if overall_score < 0.75:  # Low threshold
        return ImpactLevel.LOW, assessment_method
    elif overall_score < 1.75:  # Medium threshold
        return ImpactLevel.MEDIUM, assessment_method
    else:
        return ImpactLevel.HIGH, assessment_method

def determine_impact_level(dataset_size: int | None, 
                          downloads: int | None = None, 
                          likes: int | None = None) -> ImpactLevel:
    """
    Determine the impact level of a dataset based on size and popularity metrics.
    
    Args:
        dataset_size: Size of the dataset in bytes, or None if size is unknown
        downloads: Number of downloads (all time), or None if unknown
        likes: Number of likes, or None if unknown
        
    Returns:
        Impact level: ImpactLevel.LOW, ImpactLevel.MEDIUM, or ImpactLevel.HIGH
    """
    impact_level, _ = determine_impact_level_by_criteria(dataset_size, downloads, likes)
    return impact_level

def get_dataset_size(dataset_info: dict) -> int | None:
    """
    Extract the dataset size from dataset info.
    Returns None if size information is not available.
    """
    # Try to get size from different possible locations in the dataset info
    if "size" in dataset_info:
        return dataset_info["size"]
    
    # Some datasets might store size in bytes in cardData
    if "cardData" in dataset_info and isinstance(dataset_info["cardData"], dict):
        card_data = dataset_info["cardData"]
        if "size_bytes" in card_data:
            return card_data["size_bytes"]
        if "size" in card_data:
            return card_data["size"]
    
    # Try dataset_viewer API info format
    if "dataset_size" in dataset_info:
        return dataset_info["dataset_size"]
    
    return None

def get_dataset_metrics(dataset_info: dict) -> Tuple[int | None, int | None]:
    """
    Extract downloads and likes metrics from dataset info.
    Returns (downloads, likes) tuple, with None for unavailable metrics.
    """
    downloads = None
    likes = None
    
    # Log the structure of dataset_info to help debugging
    try:
        log.debug(f"Dataset info keys: {list(dataset_info.keys())}")
        if "cardData" in dataset_info:
            log.debug(f"CardData keys: {list(dataset_info['cardData'].keys())}")
    except Exception:
        pass
    
    # Check for total downloads
    if "downloads" in dataset_info:
        downloads = dataset_info["downloads"]
    
    # Check for 'downloads_all_time' which is sometimes used instead
    if downloads is None and "downloads_all_time" in dataset_info:
        downloads = dataset_info["downloads_all_time"]
    
    # Check for alternate keys
    if downloads is None and "downloadsAllTime" in dataset_info:
        downloads = dataset_info["downloadsAllTime"]
    
    if "likes" in dataset_info:
        likes = dataset_info["likes"]
    
    # Check cardData as fallback
    if "cardData" in dataset_info and isinstance(dataset_info["cardData"], dict):
        card_data = dataset_info["cardData"]
        if downloads is None:
            # Try different possible keys for downloads
            for key in ["downloads", "downloads_all_time", "downloadsAllTime", "totalDownloads"]:
                if key in card_data:
                    downloads = card_data[key]
                    break
                    
        if likes is None and "likes" in card_data:
            likes = card_data["likes"]
    
    return downloads, likes

async def list_datasets_async(
    limit: int | None = None,
    offset: int | None = 0,
    search: str | None = None,
    include_size: bool = False,
    include_impact: bool = False,
    use_stored_impacts: bool = True,
    count_total: bool = False,
) -> dict[str, Any] | list[dict[str, Any]]:
    """
    Asynchronously fetch datasets from the HuggingFace API with optional filtering and impact assessment.
    
    Args:
        limit: Maximum number of datasets to return
        offset: Number of datasets to skip (for pagination)
        search: Optional search string to filter datasets
        include_size: Whether to include size information
        include_impact: Whether to include impact assessment
        use_stored_impacts: Whether to use stored impact assessments from database
        count_total: Whether to return total count in a paginated response format
        
    Returns:
        If count_total is True, returns a dict with 'items' (list of datasets) and 'count' (total number of matching datasets)
        Otherwise, returns a list of dataset dictionaries
    """
    async def _worker() -> dict[str, Any] | list[dict[str, Any]]:
        # Use direct requests.get for HuggingFace API
        # Fetch all datasets by using a very large limit (10000)
        # This should effectively return all available datasets in most cases
        hf_datasets = list_datasets_via_requests(limit=10000, full=include_size)
        
        # Apply filtering
        if search:
            search_lower = search.lower()
            filtered_datasets = [
                d for d in hf_datasets 
                if search_lower in d["id"].lower() or 
                search_lower in (d.get("name") or "").lower() or
                search_lower in (d.get("description") or "").lower()
            ]
        else:
            filtered_datasets = hf_datasets
        
        # Get total count before applying limit/offset
        total_count = len(filtered_datasets)
        
        # Apply pagination
        if offset is not None and offset > 0:
            filtered_datasets = filtered_datasets[offset:]
            
        if limit is not None:
            filtered_datasets = filtered_datasets[:limit]
        
        # Prepare result datasets with additional information
        results = []
        for dataset in filtered_datasets:
            # Always include id, name, and description
            result_dataset = {
                "id": dataset["id"],
                "name": dataset.get("name"),
                "description": dataset.get("description"),
                "tags": dataset.get("tags", []),
                "author": dataset.get("author"),
                "created_at": dataset.get("created_at"),
                "updated_at": dataset.get("updated_at"),
            }
            
            # Add size if requested
            if include_size and "size" in dataset:
                result_dataset["size_bytes"] = dataset["size"]
            
            results.append(result_dataset)
        
        # If we need to include impact assessment
        if include_impact:
            for dataset in results:
                dataset_id = dataset["id"]
                # Initialize empty impact assessment
                dataset["impact_level"] = ImpactLevel.LOW
                dataset["impact_assessment"] = {"source": "default"}
                
                try:
                    # Try to get stored impact assessment
                    stored_impact = await get_stored_dataset_impact(dataset_id)
                    if stored_impact:
                        # Update with stored impact data
                        dataset["impact_level"] = ImpactLevel(stored_impact["impact_level"])
                        dataset["impact_assessment"]["source"] = "database"
                        dataset["impact_assessment"]["method"] = stored_impact["assessment_method"]
                        dataset["impact_assessment"]["metrics"] = {
                            "size_bytes": stored_impact.get("metrics", {}).get("size_bytes"),
                            "file_count": stored_impact.get("metrics", {}).get("file_count"),
                            "downloads": stored_impact.get("metrics", {}).get("downloads"),
                            "likes": stored_impact.get("metrics", {}).get("likes")
                        }
                except Exception as e:
                    log.warning(f"Failed to fetch stored impact for {dataset_id}: {e}")
        
        if count_total:
            return {
                "items": results,
                "count": total_count
            }
        else:
            return results
            
    if include_impact:
        # For impact assessment, we might need async operations
        return await _worker()
    else:
        # Just return basic dataset info
        return await anyio.to_thread.run_sync(_worker)

async def get_dataset_impact_async(dataset_id: str) -> Dict[str, Any]:
    """
    Get comprehensive impact assessment for a dataset.
    
    This function fetches dataset information from multiple sources and
    computes an impact assessment based on size, downloads, and likes.
    
    Returns:
        Dictionary with impact assessment details
    """
    # First try to get info from dataset-viewer API
    viewer_info = await anyio.to_thread.run_sync(fetch_dataset_viewer_info, dataset_id)
    
    # Get size information
    dataset_size = get_dataset_size(viewer_info)
    
    # Get files for fallback size estimation
    files = await list_repo_files_async(dataset_id)
    file_count = len(files)
    
    # Try to get download and likes metrics from multiple sources
    downloads = None
    likes = None
    
    # First try direct metrics API
    try:
        metrics_data = await anyio.to_thread.run_sync(
            lambda: fetch_dataset_metrics(dataset_id)
        )
        if metrics_data:
            downloads = metrics_data.get("downloads")
            # If all-time downloads not available, try monthly
            if downloads is None:
                downloads = metrics_data.get("monthly_downloads")
            likes = metrics_data.get("likes")
            log.info(f"Retrieved metrics for {dataset_id}: downloads={downloads}, likes={likes}")
    except Exception as e:
        log.warning(f"Failed to fetch metrics: {e}")
    
    # Try through HF API if still no metrics
    if downloads is None or likes is None:
        try:
            hf_info = api.dataset_info(dataset_id, token=get_hf_token())
            hf_info_dict = _dataset_info_to_dict(hf_info) if hf_info else {}
            
            d, l = get_dataset_metrics(hf_info_dict)
            if downloads is None:
                downloads = d
            if likes is None:
                likes = l
        except Exception as e:
            log.warning(f"Failed to fetch HF API dataset info: {e}")
    
    # If we couldn't get any size info, estimate based on file count
    if dataset_size is None and file_count > 0:
        # Very rough estimation - just for fallback
        avg_file_size = 5 * 1024 * 1024  # Assume 5MB average per file
        dataset_size = avg_file_size * file_count
    
    # Determine impact level
    impact_level, method = determine_impact_level_by_criteria(dataset_size, downloads, likes)
    
    # Log the metrics we're using
    log.info(f"Impact assessment for {dataset_id}: size={dataset_size}, downloads={downloads}, likes={likes}, level={impact_level}")
    
    # Construct thresholds info
    thresholds = {
        "size": {
            "low": f"< {SIZE_THRESHOLD_LOW / (1024 * 1024):.0f} MB",
            "medium": f"{SIZE_THRESHOLD_LOW / (1024 * 1024):.0f} MB - {SIZE_THRESHOLD_MEDIUM / (1024 * 1024 * 1024):.0f} GB",
            "high": f"> {SIZE_THRESHOLD_MEDIUM / (1024 * 1024 * 1024):.0f} GB"
        },
        "downloads": {
            "low": f"< {DOWNLOADS_THRESHOLD_LOW}",
            "medium": f"{DOWNLOADS_THRESHOLD_LOW} - {DOWNLOADS_THRESHOLD_MEDIUM}",
            "high": f"> {DOWNLOADS_THRESHOLD_MEDIUM}"
        },
        "likes": {
            "low": f"< {LIKES_THRESHOLD_LOW}",
            "medium": f"{LIKES_THRESHOLD_LOW} - {LIKES_THRESHOLD_MEDIUM}",
            "high": f"> {LIKES_THRESHOLD_MEDIUM}"
        },
        "file_count": {
            "low": "1-5 files",
            "medium": "6-20 files",
            "high": "20+ files"
        }
    }
    
    return {
        "dataset_id": dataset_id,
        "impact_level": impact_level,
        "assessment_method": method,
        "metrics": {
            "size_bytes": dataset_size,
            "downloads": downloads,
            "likes": likes,
            "file_count": file_count
        },
        "thresholds": thresholds
    }

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
    "get_file_download_url_async",
    "determine_impact_level",
    "get_dataset_impact_async"
]

