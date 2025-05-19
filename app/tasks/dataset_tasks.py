import logging
import time
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from celery import Task, shared_task
from app.core.celery_app import get_celery_app
from app.services.hf_datasets import (
    determine_impact_level_by_criteria,
    get_hf_token,
    get_dataset_size,
    refresh_datasets_cache,
    fetch_and_cache_all_datasets,
)
from app.services.redis_client import sync_cache_set, sync_cache_get, generate_cache_key
from app.core.config import settings
import requests
import os

# Configure logging
logger = logging.getLogger(__name__)

# Get Celery app instance
celery_app = get_celery_app()

# Constants
DATASET_CACHE_TTL = 60 * 60 * 24 * 30  # 30 days
BATCH_PROGRESS_CACHE_TTL = 60 * 60 * 24 * 7  # 7 days for batch progress
DATASET_SIZE_CACHE_TTL = 60 * 60 * 24 * 30  # 30 days for size info

@celery_app.task(name="app.tasks.dataset_tasks.refresh_hf_datasets_cache")
def refresh_hf_datasets_cache():
    """Celery task to refresh the HuggingFace datasets cache in Redis."""
    logger.info("Starting refresh of HuggingFace datasets cache via Celery task.")
    try:
        refresh_datasets_cache()
        logger.info("Successfully refreshed HuggingFace datasets cache.")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Failed to refresh HuggingFace datasets cache: {e}")
        return {"status": "error", "error": str(e)}

@shared_task(bind=True, max_retries=3, default_retry_delay=10)
def fetch_datasets_page(self, offset, limit):
    """
    Celery task to fetch and cache a single page of datasets from Hugging Face.
    Retries on failure.
    """
    logger.info(f"[fetch_datasets_page] ENTRY: offset={offset}, limit={limit}")
    try:
        from app.services.hf_datasets import process_datasets_page
        logger.info(f"[fetch_datasets_page] Calling process_datasets_page with offset={offset}, limit={limit}")
        result = process_datasets_page(offset, limit)
        logger.info(f"[fetch_datasets_page] SUCCESS: offset={offset}, limit={limit}, result={result}")
        return result
    except Exception as exc:
        logger.error(f"[fetch_datasets_page] ERROR: offset={offset}, limit={limit}, exc={exc}", exc_info=True)
        raise self.retry(exc=exc)

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def refresh_hf_datasets_full_cache(self):
    logger.info("[refresh_hf_datasets_full_cache] Starting full Hugging Face datasets cache refresh.")
    try:
        token = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
        if not token:
            logger.error("[refresh_hf_datasets_full_cache] HUGGINGFACEHUB_API_TOKEN not set.")
            return {"status": "error", "error": "HUGGINGFACEHUB_API_TOKEN not set"}
        count = fetch_and_cache_all_datasets(token)
        logger.info(f"[refresh_hf_datasets_full_cache] Cached {count} datasets.")
        return {"status": "ok", "cached": count}
    except Exception as exc:
        logger.error(f"[refresh_hf_datasets_full_cache] ERROR: {exc}", exc_info=True)
        raise self.retry(exc=exc) 