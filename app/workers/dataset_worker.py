"""Worker module for background dataset processing tasks."""

import asyncio
import logging
import uuid
import time
import json
from typing import Dict, List, Any, Optional, Set
import signal
import sys
from datetime import datetime, timezone

from app.services.redis_client import (
    get_redis,
    mark_task_complete,
    cache_set,
    cache_get
)
from app.schemas.dataset import ImpactLevel
from app.services.hf_datasets import get_dataset_impact_async
from app.services.dataset_impacts import save_dataset_impact

# Configure logging
log = logging.getLogger(__name__)

# Worker configuration
WORKER_QUEUE = "dataset_impacts"
MAX_CONCURRENT_TASKS = 5
SHUTDOWN_TIMEOUT = 30  # seconds
DATASET_CACHE_TTL = 60 * 60 * 24  # 24 hours

# Global state
_worker_id = str(uuid.uuid4())[:8]
_is_running = False
_active_tasks: Set[str] = set()

async def process_dataset(dataset_id: str, jwt: Optional[str] = None) -> Dict[str, Any]:
    """Process a single dataset's impact assessment."""
    task_id = f"{dataset_id}-{int(time.time())}"
    
    try:
        # Check if we have cached results (even if we're forcing a refresh,
        # having cache is useful for comparison)
        cache_key = f"dataset:impact:{dataset_id}"
        cached_impact = await cache_get(cache_key)
        
        # Get fresh impact assessment
        log.info(f"Worker {_worker_id} calculating impact for dataset: {dataset_id}")
        start_time = time.time()
        impact_data = await get_dataset_impact_async(dataset_id)
        duration = time.time() - start_time
        
        # Record performance metrics
        metrics = {
            "dataset_id": dataset_id,
            "duration_seconds": duration,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "worker_id": _worker_id,
            "cache_hit": cached_impact is not None
        }
        
        # Store result in database
        await save_dataset_impact(dataset_id, impact_data, jwt)
        
        # Cache the result
        await cache_set(cache_key, impact_data, expire=DATASET_CACHE_TTL)
        
        # Add result metrics
        result = {
            "dataset_id": dataset_id,
            "impact_level": impact_data["impact_level"].value 
                if hasattr(impact_data["impact_level"], "value") 
                else impact_data["impact_level"],
            "metrics": metrics,
            "status": "completed"
        }
        
        log.info(f"Worker {_worker_id} completed dataset {dataset_id} "
                 f"impact={result['impact_level']} in {duration:.2f}s")
        
        return result
    except Exception as e:
        log.error(f"Worker {_worker_id} error processing dataset {dataset_id}: {e}")
        return {
            "dataset_id": dataset_id,
            "status": "error",
            "error": str(e)
        }

async def process_batch(batch_id: str, dataset_ids: List[str], jwt: Optional[str] = None) -> Dict[str, Any]:
    """Process a batch of datasets concurrently with limited concurrency."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    
    # Create task tracking data
    total = len(dataset_ids)
    completed = 0
    results = []
    errors = []
    start_time = time.time()
    
    async def process_with_semaphore(dataset_id: str):
        nonlocal completed
        
        async with semaphore:
            result = await process_dataset(dataset_id, jwt)
            completed += 1
            
            if result.get("status") == "error":
                errors.append(result)
            
            results.append(result)
            
            # Update batch progress
            progress = {
                "batch_id": batch_id,
                "completed": completed,
                "total": total,
                "percent": int((completed / total) * 100),
                "errors": len(errors),
                "duration": time.time() - start_time
            }
            
            # Cache progress for status checks
            await cache_set(f"batch:progress:{batch_id}", progress, expire=86400)
    
    # Create tasks for all datasets
    tasks = [process_with_semaphore(dataset_id) for dataset_id in dataset_ids]
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)
    
    # Create final result
    duration = time.time() - start_time
    result = {
        "batch_id": batch_id,
        "datasets_processed": len(results),
        "successful": len(results) - len(errors),
        "errors": len(errors),
        "duration_seconds": duration,
        "datasets": results
    }
    
    log.info(f"Worker {_worker_id} completed batch {batch_id}: "
             f"{len(results)} datasets in {duration:.2f}s with {len(errors)} errors")
    
    return result

async def worker_task(batch_id: str, dataset_ids: List[str], jwt: Optional[str] = None) -> None:
    """Process a batch of datasets and mark task as complete."""
    _active_tasks.add(batch_id)
    
    try:
        # Process the batch
        result = await process_batch(batch_id, dataset_ids, jwt)
        
        # Mark task as complete
        await mark_task_complete(WORKER_QUEUE, batch_id, result)
    except Exception as e:
        log.error(f"Worker {_worker_id} error processing batch {batch_id}: {e}")
        # Mark task as error
        await mark_task_complete(WORKER_QUEUE, batch_id, {
            "batch_id": batch_id,
            "status": "error",
            "error": str(e)
        })
    finally:
        _active_tasks.remove(batch_id)

async def fetch_and_process_task() -> bool:
    """Fetch a single task from the queue and process it."""
    try:
        redis_client = await get_redis()
        
        # Pop task from queue with timeout
        result = await redis_client.brpop(f"queue:{WORKER_QUEUE}", timeout=1)
        if not result:
            return False
            
        # Parse task data
        _, task_data = result
        payload = json.loads(task_data)
        
        batch_id = payload.get("batch_id")
        dataset_ids = payload.get("dataset_ids", [])
        jwt = payload.get("jwt")
        
        log.info(f"Worker {_worker_id} processing batch {batch_id} with {len(dataset_ids)} datasets")
        
        # Create background task to process batch
        asyncio.create_task(worker_task(batch_id, dataset_ids, jwt))
        
        return True
    except Exception as e:
        log.error(f"Worker {_worker_id} error fetching task: {e}")
        return False

async def worker_loop() -> None:
    """Main worker loop that processes tasks from the queue."""
    global _is_running
    
    log.info(f"Starting dataset impact worker {_worker_id}")
    _is_running = True
    
    try:
        while _is_running:
            # Fetch and process a single task
            task_processed = await fetch_and_process_task()
            
            # If no task was processed, wait a bit
            if not task_processed:
                await asyncio.sleep(0.1)
    except Exception as e:
        log.error(f"Worker {_worker_id} error in main loop: {e}")
    finally:
        log.info(f"Worker {_worker_id} shutting down")
        _is_running = False

async def shutdown(signal_received=None) -> None:
    """Gracefully shutdown worker."""
    global _is_running
    
    if signal_received:
        log.info(f"Worker {_worker_id} received signal: {signal_received}")
    
    if not _is_running:
        return
        
    log.info(f"Worker {_worker_id} shutting down gracefully. "
             f"Active tasks: {len(_active_tasks)}")
    
    # Stop accepting new tasks
    _is_running = False
    
    # Wait for active tasks with timeout
    shutdown_timeout = SHUTDOWN_TIMEOUT
    while _active_tasks and shutdown_timeout > 0:
        log.info(f"Waiting for {len(_active_tasks)} active tasks to complete. "
                 f"Timeout in {shutdown_timeout}s")
        await asyncio.sleep(1)
        shutdown_timeout -= 1
    
    # Force exit if tasks didn't complete
    if _active_tasks:
        log.warning(f"Forcing shutdown with {len(_active_tasks)} active tasks")

def run_worker():
    """Run the worker process."""
    # Import here to allow using the function outside FastAPI context
    import asyncio
    
    # Setup signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda s, _: asyncio.create_task(shutdown(s)))
        
    # Run the worker
    try:
        asyncio.run(worker_loop())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.error(f"Worker {_worker_id} fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Run worker
    run_worker() 