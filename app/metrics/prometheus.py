import logging
import time
from contextlib import contextmanager
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

log = logging.getLogger(__name__)
request_count = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)
request_duration = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)
cache_hits = Counter(
    'cache_hits_total',
    'Total cache hits',
    ['cache_type']
)
cache_misses = Counter(
    'cache_misses_total',
    'Total cache misses',
    ['cache_type']
)
cache_size = Gauge(
    'cache_size_items',
    'Number of items in cache',
    ['cache_type']
)
redis_operations = Counter(
    'redis_operations_total',
    'Total Redis operations',
    ['operation', 'status']
)
redis_operation_duration = Histogram(
    'redis_operation_duration_seconds',
    'Redis operation duration in seconds',
    ['operation']
)
redis_connection_errors = Counter(
    'redis_connection_errors_total',
    'Total Redis connection errors'
)
celery_tasks_total = Counter(
    'celery_tasks_total',
    'Total Celery tasks',
    ['task_name', 'status']
)
celery_task_duration = Histogram(
    'celery_task_duration_seconds',
    'Celery task duration in seconds',
    ['task_name']
)
celery_task_retries = Counter(
    'celery_task_retries_total',
    'Total Celery task retries',
    ['task_name']
)
memory_usage_bytes = Gauge(
    'memory_usage_bytes',
    'Current memory usage in bytes',
    ['type']
)
gc_collections_total = Counter(
    'gc_collections_total',
    'Total garbage collections',
    ['generation']
)
hf_api_requests = Counter(
    'hf_api_requests_total',
    'Total HuggingFace API requests',
    ['endpoint', 'status']
)
hf_api_request_duration = Histogram(
    'hf_api_request_duration_seconds',
    'HuggingFace API request duration',
    ['endpoint']
)
hf_api_rate_limits = Counter(
    'hf_api_rate_limits_total',
    'Total HuggingFace API rate limit hits'
)
datasets_cached = Gauge(
    'datasets_cached_total',
    'Total datasets in cache'
)
datasets_processing_duration = Histogram(
    'datasets_processing_duration_seconds',
    'Dataset processing duration',
    ['operation']
)
search_requests = Counter(
    'search_requests_total',
    'Total search requests',
    ['search_type']
)
search_duration = Histogram(
    'search_duration_seconds',
    'Search request duration',
    ['search_type']
)


@contextmanager
def track_duration(histogram, labels=None):
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        if labels:
            histogram.labels(**labels).observe(duration)
        else:
            histogram.observe(duration)


def track_cache_access(cache_type, hit):
    if hit:
        cache_hits.labels(cache_type=cache_type).inc()
    else:
        cache_misses.labels(cache_type=cache_type).inc()


def update_memory_metrics():
    try:
        import psutil
        import gc
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_usage_bytes.labels(type='rss').set(memory_info.rss)
        memory_usage_bytes.labels(type='vms').set(memory_info.vms)
        for i, count in enumerate(gc.get_count()):
            gc_collections_total.labels(generation=str(i)).inc(count)
    except Exception as exc:
        log.error(f"Failed to update memory metrics: {exc}")


def track_redis_operation(operation, success, duration=None):
    status = 'success' if success else 'error'
    redis_operations.labels(operation=operation, status=status).inc()
    if duration is not None:
        redis_operation_duration.labels(operation=operation).observe(duration)
    if not success:
        redis_connection_errors.inc()


def track_celery_task(task_name, status, duration=None):
    celery_tasks_total.labels(task_name=task_name, status=status).inc()
    if duration is not None:
        celery_task_duration.labels(task_name=task_name).observe(duration)
    if status == 'retry':
        celery_task_retries.labels(task_name=task_name).inc()


def track_hf_api_request(endpoint, status_code, duration=None):
    status = 'success' if 200 <= status_code < 300 else 'error'
    hf_api_requests.labels(endpoint=endpoint, status=status).inc()
    if duration is not None:
        hf_api_request_duration.labels(endpoint=endpoint).observe(duration)
    if status_code == 429:
        hf_api_rate_limits.inc()


def get_metrics():
    update_memory_metrics()
    return generate_latest()


async def metrics_endpoint():
    from fastapi import Response
    metrics = get_metrics()
    return Response(content=metrics, media_type=CONTENT_TYPE_LATEST)