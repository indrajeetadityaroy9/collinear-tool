"""Celery configuration for task processing."""

import importlib
import logging
from celery import Celery
from celery.signals import task_failure, task_success, worker_ready, worker_shutdown

from app.core.config import settings


logger = logging.getLogger(__name__)


celery_app = Celery(
    "dataset_impacts",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)


celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_concurrency=settings.WORKER_CONCURRENCY,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_time_limit=3600,
    task_soft_time_limit=3000,
    worker_prefetch_multiplier=1,
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=10,
    broker_pool_limit=10,
    result_expires=60 * 60 * 24,
    task_track_started=True,
)


celery_app.conf.task_routes = {
    "app.tasks.dataset_tasks.*": {"queue": "dataset_impacts"},
}


celery_app.conf.task_default_retry_delay = 30
celery_app.conf.task_max_retries = 3


celery_app.conf.beat_schedule = {
    "refresh-hf-datasets-cache": {
        "task": "app.tasks.dataset_tasks.refresh_hf_datasets_cache",
        "schedule": 3600.0,
    },
}


@task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, **kwargs):
    """Log failed tasks."""
    logger.error(f"Task {task_id} {sender.name} failed: {exception}")


@task_success.connect
def task_success_handler(sender=None, result=None, **kwargs):
    """Log successful tasks."""
    task_name = sender.name if sender else "Unknown"
    logger.info(f"Task {task_name} completed successfully")


@worker_ready.connect
def worker_ready_handler(**kwargs):
    """Log when worker is ready."""
    logger.info(f"Celery worker ready: {kwargs.get('hostname')}")


@worker_shutdown.connect
def worker_shutdown_handler(**kwargs):
    """Log when worker is shutting down."""
    logger.info(f"Celery worker shutting down: {kwargs.get('hostname')}")


def get_celery_app():
    """Get the Celery app instance."""

    try:
        importlib.import_module("app.tasks")
        logger.info(
            f"Tasks successfully imported; registered {len(celery_app.tasks)} tasks"
        )
    except ImportError as e:
        logger.error(f"Error importing tasks: {e}")

    return celery_app
