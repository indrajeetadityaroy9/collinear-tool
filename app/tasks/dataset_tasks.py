import logging
import time
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from celery import Task
from app.core.celery_app import get_celery_app
from app.services.hf_datasets import (
    determine_impact_level_by_criteria,
    get_hf_token,
    get_dataset_size,
    refresh_datasets_cache,
)
from app.services.redis_client import sync_cache_set, sync_cache_get, generate_cache_key
from app.core.config import settings
import requests

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