import json
from datetime import datetime
import logging
from time import time as _time
import redis.asyncio as redis_async
import redis as redis_sync
from tenacity import retry, stop_after_attempt, wait_exponential
from app.core.config import settings
log = logging.getLogger(__name__)
_redis_pool_async = None
_redis_pool_sync = None
DEFAULT_CACHE_EXPIRY = 60 * 60 * 12

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
async def get_redis_pool():
    global _redis_pool_async
    if _redis_pool_async is None:
        redis_url = settings.REDIS_URL or 'redis://localhost:6379/0'
        try:
            _redis_pool_async = redis_async.ConnectionPool.from_url(redis_url, max_connections=10, decode_responses=True, health_check_interval=5, socket_connect_timeout=5, socket_keepalive=True, retry_on_timeout=True)
            log.info(f'Created async Redis connection pool with URL: {redis_url}')
        except Exception as e:
            log.error(f'Error creating async Redis connection pool: {e}')
            raise
    return redis_async.Redis(connection_pool=_redis_pool_async)

def get_redis_pool_sync():
    global _redis_pool_sync
    if _redis_pool_sync is None:
        redis_url = settings.REDIS_URL or 'redis://localhost:6379/0'
        try:
            _redis_pool_sync = redis_sync.ConnectionPool.from_url(redis_url, max_connections=10, decode_responses=True, socket_connect_timeout=5, socket_keepalive=True, retry_on_timeout=True)
            log.info(f'Created sync Redis connection pool with URL: {redis_url}')
        except Exception as e:
            log.error(f'Error creating sync Redis connection pool: {e}')
            raise
    return redis_sync.Redis(connection_pool=_redis_pool_sync)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
async def get_redis():
    try:
        redis_client = await get_redis_pool()
        return redis_client
    except Exception as e:
        log.error(f'Error getting Redis client: {e}')
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def get_redis_sync():
    try:
        return get_redis_pool_sync()
    except Exception as e:
        log.error(f'Error getting synchronous Redis client: {e}')
        raise

def generate_cache_key(prefix, *args):
    key_parts = [prefix] + [str(arg) for arg in args if arg]
    return ':'.join(key_parts)

def _json_serialize(obj):

    def _default(value):
        if isinstance(value, datetime):
            return value.isoformat()
        if hasattr(value, 'model_dump'):
            return value.model_dump()  # pragma: no cover (fallback if present)
        if hasattr(value, '__dict__'):
            return value.__dict__
        return str(value)

    return json.dumps(obj, default=_default)


def _json_deserialize(data):
    return json.loads(data)

async def cache_set(key, value, expire=DEFAULT_CACHE_EXPIRY):
    redis_client = await get_redis()
    serialized = _json_serialize(value)
    try:
        await redis_client.set(key, serialized, ex=expire)
        log.debug(f'Cached data at key: {key}, expires in {expire}s')
        return True
    except Exception as e:
        log.error(f'Error caching data at key {key}: {e}')
        return False

async def cache_get(key):
    redis_client = await get_redis()
    try:
        data = await redis_client.get(key)
        if not data:
            return None
        log.debug(f'Cache hit for key: {key}')
        return _json_deserialize(data)
    except Exception as e:
        log.error(f'Error retrieving cache for key {key}: {e}')
        return None

def sync_cache_set(key, value, expire=DEFAULT_CACHE_EXPIRY):
    redis_client = get_redis_sync()
    serialized = _json_serialize(value)
    start = _time()
    try:
        redis_client.set(key, serialized, ex=expire)
        elapsed = _time() - start
        if elapsed > 2:
            log.warning(f'Slow sync_cache_set for key {key}: {elapsed:.2f}s')
        log.debug(f'Cached data at key: {key}, expires in {expire}s (sync)')
        return True
    except Exception as e:
        log.error(f'Error caching data at key {key}: {e}')
        return False

def sync_cache_get(key):
    redis_client = get_redis_sync()
    start = _time()
    try:
        data = redis_client.get(key)
        elapsed = _time() - start
        if elapsed > 2:
            log.warning(f'Slow sync_cache_get for key {key}: {elapsed:.2f}s')
        if not data:
            return None
        log.debug(f'Cache hit for key: {key} (sync)')
        return _json_deserialize(data)
    except Exception as e:
        log.error(f'Error retrieving cache for key {key}: {e}')
        return None

async def cache_invalidate(key):
    redis_client = await get_redis()
    try:
        await redis_client.delete(key)
        log.debug(f'Invalidated cache for key: {key}')
        return True
    except Exception as e:
        log.error(f'Error invalidating cache for key {key}: {e}')
        return False

async def cache_invalidate_pattern(pattern):
    redis_client = await get_redis()
    try:
        keys = await redis_client.keys(pattern)
        if not keys:
            return 0
        count = await redis_client.delete(*keys)
        log.debug(f'Invalidated {count} keys matching pattern: {pattern}')
        return count
    except Exception as e:
        log.error(f'Error invalidating keys with pattern {pattern}: {e}')
        return 0

async def enqueue_task(queue_name, task_id, payload):
    redis_client = await get_redis()
    try:
        serialized = _json_serialize(payload)
        await redis_client.lpush(f'queue:{queue_name}', serialized)
        await redis_client.hset(f'tasks:{queue_name}', task_id, 'pending')
        log.info(f'Enqueued task {task_id} to queue {queue_name}')
        return True
    except Exception as e:
        log.error(f'Error enqueueing task {task_id} to {queue_name}: {e}')
        return False

async def mark_task_complete(queue_name, task_id, result=None):
    redis_client = await get_redis()
    try:
        if result:
            await redis_client.hset(f'results:{queue_name}', task_id, _json_serialize(result))
        await redis_client.hset(f'tasks:{queue_name}', task_id, 'complete')
        await redis_client.expire(f'tasks:{queue_name}', 86400)
        log.info(f'Marked task {task_id} as complete in queue {queue_name}')
        return True
    except Exception as e:
        log.error(f'Error marking task {task_id} as complete: {e}')
        return False

async def get_task_status(queue_name, task_id):
    redis_client = await get_redis()
    try:
        status = await redis_client.hget(f'tasks:{queue_name}', task_id)
        return status
    except Exception as e:
        log.error(f'Error getting status for task {task_id}: {e}')
        return None

async def get_task_result(queue_name, task_id):
    redis_client = await get_redis()
    try:
        data = await redis_client.hget(f'results:{queue_name}', task_id)
        if not data:
            return None
        return _json_deserialize(data)
    except Exception as e:
        log.error(f'Error getting result for task {task_id}: {e}')
        return None

async def add_to_stream(stream, data, max_len=1000):
    redis_client = await get_redis()
    try:
        entry = {k: _json_serialize(v) for (k, v) in data.items()}
        event_id = await redis_client.xadd(stream, entry, maxlen=max_len, approximate=True)
        log.debug(f'Added event {event_id} to stream {stream}')
        return event_id
    except Exception as e:
        log.error(f'Error adding to stream {stream}: {e}')
        raise
