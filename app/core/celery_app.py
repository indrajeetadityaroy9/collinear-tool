"""Celery configuration for task processing."""

import logging
import os
from celery import Celery
from kombu.utils.url import maybe_sanitize_url
from celery.signals import task_failure, task_success, worker_ready, worker_shutdown

from app.core.config import settings

# Configure logging
logger = logging.getLogger(__name__)

# Celery configuration
celery_app = Celery(
    "dataset_impacts",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

# Configure Celery settings
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_concurrency=settings.WORKER_CONCURRENCY,
    task_acks_late=True,  # Tasks are acknowledged after execution
    task_reject_on_worker_lost=True,  # Tasks are rejected if worker is terminated during execution
    task_time_limit=3600,  # 1 hour timeout per task
    task_soft_time_limit=3000,  # Soft timeout (30 minutes) - allows for graceful shutdown
    worker_prefetch_multiplier=1,  # Single prefetch - improves fair distribution of tasks
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=10,
    broker_pool_limit=10,  # Connection pool size
)

# Set up task routes for different task types
celery_app.conf.task_routes = {
    "app.tasks.dataset_tasks.*": {"queue": "dataset_impacts"},
    "app.tasks.maintenance.*": {"queue": "maintenance"},
}

# Configure retry settings
celery_app.conf.task_default_retry_delay = 30  # 30 seconds
celery_app.conf.task_max_retries = 3

# Setup beat schedule for periodic tasks if enabled
celery_app.conf.beat_schedule = {
    "cleanup-stale-tasks": {
        "task": "app.tasks.maintenance.cleanup_stale_tasks",
        "schedule": 3600.0,  # Run every hour
    },
    "health-check": {
        "task": "app.tasks.maintenance.health_check",
        "schedule": 300.0,  # Run every 5 minutes
    },
}

# Signal handlers for monitoring and logging
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

# Expose the app for importing in other modules
def get_celery_app():
    """Get the Celery app instance."""
    return celery_app 