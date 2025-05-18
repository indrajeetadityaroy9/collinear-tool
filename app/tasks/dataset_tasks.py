"""Celery tasks for dataset processing."""

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from celery import Task, states
from celery.exceptions import Retry, MaxRetriesExceededError
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.celery_app import get_celery_app
from app.services.hf_datasets import get_dataset_impact_async
from app.services.dataset_impacts import save_dataset_impact, get_stored_dataset_impact
from app.services.redis_client import cache_set, cache_get, generate_cache_key

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
    Process a batch of datasets for impact assessment.
    
    Args:
        batch_id: Unique ID for the batch
        dataset_ids: List of dataset IDs to process
        force_refresh: Whether to force fresh assessments
        jwt: Optional JWT for authenticated database access
        
    Returns:
        Dictionary with batch processing results
    """
    logger.info(f"Processing batch {batch_id} with {len(dataset_ids)} datasets")
    start_time = time.time()
    
    # Initialize progress tracking
    total = len(dataset_ids)
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
    
    # Store initial progress
    cache_key = generate_cache_key("batch:progress", batch_id)
    cache_set(cache_key, progress, expire=BATCH_PROGRESS_CACHE_TTL)
    
    # Process datasets in chunks to avoid overwhelming memory
    chunk_size = 50
    all_results = []
    completed = 0
    errors = 0
    
    for i in range(0, total, chunk_size):
        chunk = dataset_ids[i:i+chunk_size]
        chunk_tasks = []
        
        # Create tasks for all datasets in chunk
        for dataset_id in chunk:
            task = process_dataset_impact.apply_async(
                args=[dataset_id, force_refresh, jwt],
                task_id=f"{batch_id}:{dataset_id}",
            )
            chunk_tasks.append((dataset_id, task.id))
            progress["tasks"][dataset_id] = {
                "task_id": task.id,
                "status": "pending"
            }
        
        # Update progress
        cache_set(cache_key, progress, expire=BATCH_PROGRESS_CACHE_TTL)
        
        # Wait for all tasks in chunk to complete
        for dataset_id, task_id in chunk_tasks:
            try:
                result = process_dataset_impact.AsyncResult(task_id).get(timeout=600)  # 10 minute timeout
                all_results.append(result)
                completed += 1
                
                if result.get("status") == "error":
                    errors += 1
                    
                # Update progress
                progress["completed"] = completed
                progress["percent"] = int((completed / total) * 100)
                progress["errors"] = errors
                progress["duration"] = time.time() - start_time
                progress["tasks"][dataset_id]["status"] = result.get("status", "completed")
                cache_set(cache_key, progress, expire=BATCH_PROGRESS_CACHE_TTL)
                
            except Exception as exc:
                logger.exception(f"Error waiting for task {task_id}: {exc}")
                completed += 1
                errors += 1
                
                # Update progress
                progress["completed"] = completed
                progress["percent"] = int((completed / total) * 100)
                progress["errors"] = errors
                progress["duration"] = time.time() - start_time
                progress["tasks"][dataset_id]["status"] = "error"
                cache_set(cache_key, progress, expire=BATCH_PROGRESS_CACHE_TTL)
    
    # Finalize batch progress
    duration = time.time() - start_time
    progress["status"] = "completed"
    progress["duration"] = duration
    cache_set(cache_key, progress, expire=BATCH_PROGRESS_CACHE_TTL)
    
    # Create final result
    result = {
        "batch_id": batch_id,
        "datasets_processed": len(all_results),
        "successful": len(all_results) - errors,
        "errors": errors,
        "duration_seconds": duration,
        "datasets": all_results
    }
    
    logger.info(f"Completed batch {batch_id}: "
                f"{len(all_results)} datasets in {duration:.2f}s with {errors} errors")
    
    return result 