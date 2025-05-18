"""Maintenance tasks for system health and cleanup operations."""

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

from app.core.celery_app import get_celery_app
from app.services.redis_client import get_redis, cache_invalidate_pattern
from app.tasks.dataset_tasks import DatasetImpactTask

# Configure logging
logger = logging.getLogger(__name__)

# Get Celery app instance
celery_app = get_celery_app()

@celery_app.task(name="app.tasks.maintenance.cleanup_stale_tasks")
async def cleanup_stale_tasks() -> Dict[str, Any]:
    """
    Clean up stale task results and progress tracking.
    
    This task:
    1. Finds and removes old batch progress records
    2. Finds and removes old task error records
    3. Removes stale task results from Redis
    
    Returns:
        Dictionary with cleanup statistics
    """
    start_time = time.time()
    logger.info("Starting cleanup of stale tasks")
    
    # Get Redis client
    redis_client = await get_redis()
    
    # Cleanup old batch progress (older than 7 days)
    batch_cleanup_count = await cache_invalidate_pattern("batch:progress:*:*")
    logger.info(f"Cleaned up {batch_cleanup_count} stale batch progress records")
    
    # Cleanup old task errors (older than 7 days)
    error_cleanup_count = await cache_invalidate_pattern("task:error:*:*")
    logger.info(f"Cleaned up {error_cleanup_count} stale task error records")
    
    # Cleanup old Celery task results
    # Find all keys in the Celery results backend
    celery_keys = await redis_client.keys("celery-task-meta-*")
    
    # Delete keys older than 7 days
    celery_cleanup_count = 0
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=7)
    
    for key in celery_keys:
        try:
            # Check the time field in the result
            result_data = await redis_client.get(key)
            if result_data:
                # If the result is older than cutoff, delete it
                await redis_client.delete(key)
                celery_cleanup_count += 1
        except Exception as e:
            logger.error(f"Error cleaning up Celery result key {key}: {e}")
    
    logger.info(f"Cleaned up {celery_cleanup_count} stale Celery task results")
    
    # Return cleanup statistics
    duration = time.time() - start_time
    return {
        "batch_progress_cleaned": batch_cleanup_count,
        "task_errors_cleaned": error_cleanup_count,
        "celery_results_cleaned": celery_cleanup_count,
        "duration_seconds": duration,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@celery_app.task(name="app.tasks.maintenance.health_check")
async def health_check() -> Dict[str, Any]:
    """
    Perform system health check.
    
    This task:
    1. Checks Redis connectivity
    2. Monitors worker stats
    3. Reports system health metrics
    
    Returns:
        Dictionary with health status information
    """
    start_time = time.time()
    logger.info("Starting system health check")
    
    health_status = {
        "status": "healthy",
        "services": {},
        "workers": {},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    # Check Redis connectivity
    try:
        redis_client = await get_redis()
        await redis_client.ping()
        health_status["services"]["redis"] = {
            "status": "available",
            "connection_active": True
        }
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        health_status["services"]["redis"] = {
            "status": "unavailable",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check Celery workers
    try:
        # Get stats from custom task class
        worker_stats = DatasetImpactTask.get_stats()
        health_status["workers"]["dataset_impact"] = {
            "processed": worker_stats["processed"],
            "errors": worker_stats["errors"],
            "cache_hits": worker_stats["cache_hits"],
            "error_rate": worker_stats["errors"] / max(worker_stats["processed"], 1)
        }
        
        # Check if error rate is too high
        if worker_stats["processed"] > 0 and worker_stats["errors"] / worker_stats["processed"] > 0.2:
            logger.warning(f"High error rate in dataset impact tasks: {worker_stats['errors']} errors out of {worker_stats['processed']} tasks")
            health_status["status"] = "degraded"
    except Exception as e:
        logger.error(f"Worker health check failed: {e}")
        health_status["workers"]["error"] = str(e)
        health_status["status"] = "degraded"
    
    # Measure response time
    duration = time.time() - start_time
    health_status["response_time"] = duration
    
    # Store health check result in Redis for monitoring
    redis_client = await get_redis()
    await redis_client.set(
        "system:health:latest", 
        str(health_status),
        ex=3600  # Keep for 1 hour
    )
    
    # Also store in a time series
    await redis_client.lpush(
        "system:health:history",
        str({**health_status, "timestamp": datetime.now(timezone.utc).isoformat()})
    )
    
    # Trim history to last 100 entries
    await redis_client.ltrim("system:health:history", 0, 99)
    
    logger.info(f"Health check completed in {duration:.4f}s: status={health_status['status']}")
    return health_status

@celery_app.task(name="app.tasks.maintenance.cache_warmup")
async def cache_warmup(dataset_ids: List[str] = None) -> Dict[str, Any]:
    """
    Warm up cache with frequently accessed datasets.
    
    Args:
        dataset_ids: Optional list of dataset IDs to warm up.
                     If not provided, uses top datasets.
                     
    Returns:
        Dictionary with warmup statistics
    """
    from app.services.hf_datasets import list_datasets_async
    
    start_time = time.time()
    logger.info("Starting cache warmup task")
    
    # If no dataset IDs provided, use top datasets
    if not dataset_ids:
        try:
            # Get top 50 most popular datasets
            top_datasets = await list_datasets_async(limit=50, sort_by="downloads")
            dataset_ids = [d["id"] for d in top_datasets]
            logger.info(f"Using top {len(dataset_ids)} datasets for cache warmup")
        except Exception as e:
            logger.error(f"Error fetching top datasets for cache warmup: {e}")
            return {
                "status": "error",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }
    
    # Warm up cache for each dataset
    from app.tasks.dataset_tasks import process_dataset_impact
    
    successful = 0
    failed = 0
    
    for dataset_id in dataset_ids:
        try:
            # Schedule task to process dataset
            process_dataset_impact.apply_async(
                args=[dataset_id, False, None],
                task_id=f"warmup:{dataset_id}:{int(time.time())}"
            )
            successful += 1
        except Exception as e:
            logger.error(f"Error scheduling cache warmup for {dataset_id}: {e}")
            failed += 1
    
    duration = time.time() - start_time
    logger.info(f"Scheduled cache warmup for {successful} datasets in {duration:.2f}s")
    
    return {
        "status": "completed",
        "datasets_scheduled": successful,
        "datasets_failed": failed,
        "duration_seconds": duration,
        "timestamp": datetime.now(timezone.utc).isoformat()
    } 