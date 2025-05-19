"""Celery tasks for dataset processing."""

import logging
import time
import uuid
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from celery import Task, states, group, chord
from celery.exceptions import Retry, MaxRetriesExceededError
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.celery_app import get_celery_app
from app.services.hf_datasets import get_dataset_impact_async, determine_impact_level_by_criteria
from app.services.dataset_impacts import save_dataset_impact, get_stored_dataset_impact
from app.services.redis_client import cache_set, cache_get, generate_cache_key
from app.core.config import settings

# Configure logging
logger = logging.getLogger(__name__)

# Get Celery app instance
celery_app = get_celery_app()

# Constants
DATASET_CACHE_TTL = 60 * 60 * 24  # 24 hours
BATCH_PROGRESS_CACHE_TTL = 60 * 60 * 24  # 24 hours

class DatasetImpactTask(Task):
    """Base task with error handling for dataset impact assessment."""
    
    _stats = {
        "processed": 0,
        "errors": 0,
        "cache_hits": 0,
    }
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure."""
        logger.error(f"Task {task_id} failed: {exc}")
        self._stats["errors"] += 1
        
        # Store error in Redis for monitoring
        cache_key = generate_cache_key("task:error", task_id)
        error_data = {
            "task_id": task_id,
            "task_name": self.name,
            "args": args,
            "kwargs": kwargs,
            "error": str(exc),
            "traceback": einfo.traceback if einfo else None,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        cache_set(cache_key, error_data, expire=86400)  # Store for 24 hours
        
        super().on_failure(exc, task_id, args, kwargs, einfo)
    
    def on_success(self, retval, task_id, args, kwargs):
        """Handle task success."""
        self._stats["processed"] += 1
        super().on_success(retval, task_id, args, kwargs)
    
    @classmethod
    def get_stats(cls):
        """Get task statistics."""
        return cls._stats.copy()


@celery_app.task(bind=True, base=DatasetImpactTask, name="app.tasks.dataset_tasks.process_dataset_impact")
def process_dataset_impact(self, dataset_id: str, force_refresh: bool = False, jwt: Optional[str] = None) -> Dict[str, Any]:
    """
    Process impact assessment for a single dataset.
    
    Args:
        dataset_id: Hugging Face dataset ID
        force_refresh: Whether to force a fresh assessment
        jwt: Optional JWT for authenticated database access
        
    Returns:
        Dictionary with impact assessment result
    """
    logger.info(f"Processing impact assessment for dataset: {dataset_id}")
    start_time = time.time()
    
    try:
        # Check cache first if not forcing refresh
        if not force_refresh:
            cache_key = generate_cache_key("dataset:impact", dataset_id)
            cached_impact = cache_get(cache_key)
            
            if cached_impact:
                logger.info(f"Using cached impact assessment for {dataset_id}")
                self._stats["cache_hits"] += 1
                return cached_impact
            
            # Try database next
            stored_impact = get_stored_dataset_impact(dataset_id, jwt, use_cache=True)
            if stored_impact:
                logger.info(f"Using stored impact assessment for {dataset_id}")
                return stored_impact
        
        # Calculate fresh impact assessment
        impact_data = get_dataset_impact_async(dataset_id)
        
        # Store in database
        save_dataset_impact(dataset_id, impact_data, jwt)
        
        # Cache result
        cache_key = generate_cache_key("dataset:impact", dataset_id)
        cache_set(cache_key, impact_data, expire=DATASET_CACHE_TTL)
        
        duration = time.time() - start_time
        logger.info(f"Completed impact assessment for {dataset_id} in {duration:.2f}s")
        
        # Record metrics
        metrics = {
            "dataset_id": dataset_id,
            "duration_seconds": duration,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "task_id": self.request.id,
            "cache_hit": False,
            "impact_level": impact_data["impact_level"].value 
                if hasattr(impact_data["impact_level"], "value") 
                else impact_data["impact_level"],
        }
        
        return {
            "dataset_id": dataset_id,
            "impact_level": impact_data["impact_level"].value 
                if hasattr(impact_data["impact_level"], "value") 
                else impact_data["impact_level"],
            "metrics": metrics,
            "status": "completed"
        }
    except Exception as exc:
        logger.exception(f"Error processing dataset {dataset_id}: {exc}")
        
        # Retry with backoff if appropriate
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying dataset {dataset_id} (attempt {self.request.retries + 1})")
            raise self.retry(exc=exc, countdown=self.default_retry_delay * (2 ** self.request.retries))
        
        # If max retries exceeded, return error status
        duration = time.time() - start_time
        return {
            "dataset_id": dataset_id,
            "status": "error",
            "error": str(exc),
            "metrics": {
                "dataset_id": dataset_id,
                "duration_seconds": duration,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "task_id": self.request.id,
            }
        }


@celery_app.task(bind=True, name="app.tasks.dataset_tasks.process_dataset_batch")
def process_dataset_batch(self, batch_id: str, dataset_ids: List[str], force_refresh: bool = False, jwt: Optional[str] = None) -> Dict[str, Any]:
    """
    Process a batch of datasets for impact assessment using Celery chord for parallelism.
    """
    logger.info(f"Processing batch {batch_id} with {len(dataset_ids)} datasets")
    start_time = time.time()
    total = len(dataset_ids)
    chunk_size = getattr(settings, "DATASET_BATCH_CHUNK_SIZE", 50)
    progress = {
        "batch_id": batch_id,
        "completed": 0,
        "total": total,
        "percent": 0,
        "errors": 0,
        "duration": 0,
        "status": "processing",
        "tasks": {}
    }
    cache_key = generate_cache_key("batch:progress", batch_id)
    cache_set(cache_key, progress, expire=BATCH_PROGRESS_CACHE_TTL)
    all_results = []
    completed = 0
    errors = 0
    # Split into chunks for large batches
    for i in range(0, total, chunk_size):
        chunk = dataset_ids[i:i+chunk_size]
        # Use Celery group for parallel execution
        task_group = group(
            process_dataset_impact.s(dataset_id, force_refresh, jwt) for dataset_id in chunk
        )
        # Use chord to aggregate results
        callback = aggregate_batch_results.s(batch_id, chunk, cache_key)
        chord(task_group)(callback)
    duration = time.time() - start_time
    progress["status"] = "submitted"
    progress["duration"] = duration
    cache_set(cache_key, progress, expire=BATCH_PROGRESS_CACHE_TTL)
    return {
        "batch_id": batch_id,
        "submitted": total,
        "status": "submitted",
        "duration": duration
    }

@celery_app.task(name="app.tasks.dataset_tasks.aggregate_batch_results")
def aggregate_batch_results(results, batch_id, chunk, cache_key):
    """
    Aggregates results for a batch chunk and updates progress in Redis.
    """
    progress = cache_get(cache_key) or {}
    completed = progress.get("completed", 0) + len(results)
    errors = progress.get("errors", 0) + sum(1 for r in results if r.get("status") == "error")
    for dataset_id, result in zip(chunk, results):
        if "tasks" not in progress:
            progress["tasks"] = {}
        progress["tasks"][dataset_id] = {
            "status": result.get("status", "completed"),
            "impact_level": result.get("impact_level"),
            "error": result.get("error")
        }
    progress["completed"] = completed
    progress["errors"] = errors
    progress["percent"] = int((completed / progress.get("total", 1)) * 100)
    progress["duration"] = time.time() - (progress.get("start_time") or time.time())
    if completed >= progress.get("total", 0):
        progress["status"] = "completed"
    cache_set(cache_key, progress, expire=BATCH_PROGRESS_CACHE_TTL)
    return progress

@celery_app.task(bind=True, name="app.tasks.dataset_tasks.process_combined_dataset")
def process_combined_dataset(
    self, 
    combined_id: str, 
    source_datasets: List[str], 
    combination_strategy: str, 
    filter_criteria: Optional[Dict[str, Any]] = None,
    jwt: Optional[str] = None
) -> Dict[str, Any]:
    """
    Process a dataset combination task.
    
    Args:
        combined_id: ID of the combined dataset
        source_datasets: List of source dataset IDs to combine
        combination_strategy: Strategy to use for combining ("merge", "intersect", "filter")
        filter_criteria: Optional criteria for filtering when using "filter" strategy
        jwt: Optional JWT for authenticated database access
        
    Returns:
        Dictionary with processing result
    """
    from app.services.dataset_combine import update_combined_dataset_status
    
    logger.info(f"Processing combined dataset {combined_id} with strategy: {combination_strategy}")
    start_time = time.time()
    
    try:
        # Update status to processing
        asyncio.run(update_combined_dataset_status(combined_id, "processing", jwt=jwt))
        
        # Get impact data for all source datasets
        impact_data_list = []
        for dataset_id in source_datasets:
            try:
                # Try cached/stored impact first
                stored_impact = get_stored_dataset_impact(dataset_id, jwt)
                if stored_impact:
                    impact_data_list.append(stored_impact)
                else:
                    # Calculate impact if not already stored
                    impact_data = get_dataset_impact_async(dataset_id)
                    impact_data_list.append(impact_data)
            except Exception as e:
                logger.warning(f"Could not get impact data for dataset {dataset_id}: {e}")
        
        if not impact_data_list:
            raise ValueError("Could not retrieve impact data for any source datasets")
        
        # Initialize combined metrics
        combined_size = 0
        combined_file_count = 0
        combined_downloads = 0
        combined_likes = 0
        
        # Process based on combination strategy
        if combination_strategy == "merge":
            # For merge, sum sizes and file counts
            for impact in impact_data_list:
                metrics = impact.get("metrics", {})
                combined_size += metrics.get("size_bytes", 0) or 0
                combined_file_count += metrics.get("file_count", 0) or 0
                
                # For downloads and likes, use max values to avoid overcounting
                combined_downloads = max(combined_downloads, metrics.get("downloads", 0) or 0)
                combined_likes = max(combined_likes, metrics.get("likes", 0) or 0)
            
            logger.info(f"Merged {len(impact_data_list)} datasets with total size: {combined_size} bytes")
            
        elif combination_strategy == "intersect":
            # For intersect, use minimum values
            if impact_data_list:
                combined_size = min([impact.get("metrics", {}).get("size_bytes", 0) or 0 for impact in impact_data_list])
                combined_file_count = min([impact.get("metrics", {}).get("file_count", 0) or 0 for impact in impact_data_list])
                combined_downloads = min([impact.get("metrics", {}).get("downloads", 0) or 0 for impact in impact_data_list])
                combined_likes = min([impact.get("metrics", {}).get("likes", 0) or 0 for impact in impact_data_list])
            
            logger.info(f"Intersected {len(impact_data_list)} datasets with estimated size: {combined_size} bytes")
            
        elif combination_strategy == "filter":
            # For filter, use the original dataset as a base, then apply reduction factor
            if impact_data_list:
                base_impact = impact_data_list[0]
                base_metrics = base_impact.get("metrics", {})
                
                # Estimate reduction (25% by default, but could be customized based on filter criteria)
                reduction_factor = 0.75
                if filter_criteria and "reduction_factor" in filter_criteria:
                    reduction_factor = filter_criteria["reduction_factor"]
                
                combined_size = int((base_metrics.get("size_bytes", 0) or 0) * reduction_factor)
                combined_file_count = int((base_metrics.get("file_count", 0) or 0) * reduction_factor)
                combined_downloads = base_metrics.get("downloads", 0) or 0
                combined_likes = base_metrics.get("likes", 0) or 0
            
            logger.info(f"Filtered dataset with estimated size: {combined_size} bytes (reduction: {1-reduction_factor:.1%})")
        
        # Calculate impact level
        impact_level, assessment_method = determine_impact_level_by_criteria(
            combined_size, combined_downloads, combined_likes
        )
        
        # Create metrics for updating
        from app.schemas.dataset import DatasetMetrics, ImpactLevel
        metrics = DatasetMetrics(
            size_bytes=combined_size,
            file_count=combined_file_count,
            downloads=combined_downloads,
            likes=combined_likes
        )
        
        # Update combined dataset with results
        asyncio.run(update_combined_dataset_status(
            combined_id=combined_id,
            status="completed",
            metrics=metrics,
            impact_level=impact_level,
            jwt=jwt
        ))
        
        duration = time.time() - start_time
        logger.info(f"Completed combined dataset {combined_id} in {duration:.2f}s")
        
        return {
            "combined_id": combined_id,
            "status": "completed",
            "impact_level": impact_level.value if hasattr(impact_level, "value") else impact_level,
            "metrics": {
                "size_bytes": combined_size,
                "file_count": combined_file_count,
                "downloads": combined_downloads,
                "likes": combined_likes
            },
            "duration": duration
        }
        
    except Exception as exc:
        logger.exception(f"Error processing combined dataset {combined_id}: {exc}")
        
        # Update status to failed
        try:
            asyncio.run(update_combined_dataset_status(combined_id, "failed", jwt=jwt))
        except Exception as update_error:
            logger.error(f"Failed to update combined dataset status: {update_error}")
        
        # Retry with backoff if appropriate
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying combined dataset {combined_id} (attempt {self.request.retries + 1})")
            raise self.retry(exc=exc, countdown=self.default_retry_delay * (2 ** self.request.retries))
        
        # If max retries exceeded, return error status
        duration = time.time() - start_time
        return {
            "combined_id": combined_id,
            "status": "error",
            "error": str(exc),
            "duration": duration
        } 