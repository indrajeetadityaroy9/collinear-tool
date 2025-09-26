import logging
from celery import shared_task
from app.core.celery_app import get_celery_app
from app.core.config import settings
from app.integrations.hf_datasets import fetch_and_cache_all_datasets, process_datasets_page, refresh_datasets_cache
from app.utils.task_utils import deduplicate_task, ProgressTracker

logger = logging.getLogger(__name__)
celery_app = get_celery_app()


@celery_app.task(name='app.tasks.dataset_tasks.refresh_hf_datasets_cache')
def refresh_hf_datasets_cache():
    logger.info('Starting refresh of HuggingFace datasets cache via Celery task.')
    try:
        import asyncio
        asyncio.run(refresh_datasets_cache())
        logger.info('Successfully refreshed HuggingFace datasets cache.')
        return {'status': 'success'}
    except Exception as exc:
        logger.error('Failed to refresh HuggingFace datasets cache: %s', exc)
        return {'status': 'error', 'error': str(exc)}


@shared_task(bind=True, max_retries=3, default_retry_delay=10)
def fetch_datasets_page(self, offset, limit):
    logger.info('[fetch_datasets_page] ENTRY: offset=%s, limit=%s', offset, limit)
    try:
        import asyncio
        result = asyncio.run(process_datasets_page(offset, limit))
        logger.info('[fetch_datasets_page] SUCCESS: offset=%s, limit=%s, result=%s', offset, limit, result)
        return result
    except Exception as exc:
        logger.error('[fetch_datasets_page] ERROR: offset=%s, limit=%s, exc=%s', offset, limit, exc, exc_info=True)
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
@deduplicate_task(timeout=600)
def refresh_hf_datasets_full_cache(self):
    logger.info('[refresh_hf_datasets_full_cache] Starting full Hugging Face datasets cache refresh.')
    tracker = ProgressTracker(total_items=100, task_id=self.request.id)
    tracker.update(0, "Initializing cache refresh")
    try:
        token = settings.resolve_hf_token()
        if not token:
            logger.error('[refresh_hf_datasets_full_cache] Hugging Face token not configured.')
            return {'status': 'error', 'error': 'Hugging Face token not configured'}
        tracker.update(10, "Fetching datasets from Hugging Face")
        count = fetch_and_cache_all_datasets(token)
        tracker.complete(f"Successfully cached {count} datasets")
        logger.info('[refresh_hf_datasets_full_cache] Cached %s datasets.', count)
        return {'status': 'ok', 'cached': count}
    except Exception as exc:
        logger.error('[refresh_hf_datasets_full_cache] ERROR: %s', exc, exc_info=True)
        raise self.retry(exc=exc)