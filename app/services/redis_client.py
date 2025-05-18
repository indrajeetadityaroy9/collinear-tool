"""Redis client for caching and task queue management."""

import asyncio
import json
from typing import Any, Dict, Optional, TypeVar, Generic, Callable
from datetime import datetime, timezone
import logging

import redis.asyncio as redis
from pydantic import BaseModel

from app.core.config import settings

# Type variable for cache
T = TypeVar('T')

# Configure logging
log = logging.getLogger(__name__)

# Redis connection pool for reusing connections
_redis_pool = None

# Default cache expiration (12 hours)
DEFAULT_CACHE_EXPIRY = 60 * 60 * 12

async def get_redis_pool() -> redis.Redis:
    """Get or create Redis connection pool."""
    global _redis_pool
    
    if _redis_pool is None:
        # Get Redis configuration from settings
        redis_url = settings.redis_url or "redis://localhost:6379/0"
        
        # Create connection pool with reasonable defaults
        _redis_pool = redis.ConnectionPool.from_url(
            redis_url,
            max_connections=10,
            decode_responses=True
        )
        log.info(f"Created Redis connection pool with URL: {redis_url}")
    
    return redis.Redis(connection_pool=_redis_pool)

async def get_redis() -> redis.Redis:
    """Get Redis client from pool."""
    return await get_redis_pool()

# Cache key generation
def generate_cache_key(prefix: str, *args: Any) -> str:
    """Generate cache key with prefix and args."""
    key_parts = [prefix] + [str(arg) for arg in args if arg]
    return ":".join(key_parts)

# JSON serialization helpers
def _json_serialize(obj: Any) -> str:
    """Serialize object to JSON with datetime support."""
    def _serialize_datetime(o: Any) -> str:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, BaseModel):
            return o.dict()
        return str(o)
    
    return json.dumps(obj, default=_serialize_datetime)

def _json_deserialize(data: str, model_class: Optional[type] = None) -> Any:
    """Deserialize JSON string to object with datetime support."""
    result = json.loads(data)
    
    if model_class and issubclass(model_class, BaseModel):
        return model_class.parse_obj(result)
    
    return result

# Cache operations
async def cache_set(key: str, value: Any, expire: int = DEFAULT_CACHE_EXPIRY) -> bool:
    """Set cache value with expiration."""
    redis_client = await get_redis()
    serialized = _json_serialize(value)
    
    try:
        await redis_client.set(key, serialized, ex=expire)
        log.debug(f"Cached data at key: {key}, expires in {expire}s")
        return True
    except Exception as e:
        log.error(f"Error caching data at key {key}: {e}")
        return False

async def cache_get(key: str, model_class: Optional[type] = None) -> Optional[Any]:
    """Get cache value with optional model deserialization."""
    redis_client = await get_redis()
    
    try:
        data = await redis_client.get(key)
        if not data:
            return None
            
        log.debug(f"Cache hit for key: {key}")
        return _json_deserialize(data, model_class)
    except Exception as e:
        log.error(f"Error retrieving cache for key {key}: {e}")
        return None

async def cache_invalidate(key: str) -> bool:
    """Invalidate cache for key."""
    redis_client = await get_redis()
    
    try:
        await redis_client.delete(key)
        log.debug(f"Invalidated cache for key: {key}")
        return True
    except Exception as e:
        log.error(f"Error invalidating cache for key {key}: {e}")
        return False

async def cache_invalidate_pattern(pattern: str) -> int:
    """Invalidate all cache keys matching pattern."""
    redis_client = await get_redis()
    
    try:
        keys = await redis_client.keys(pattern)
        if not keys:
            return 0
            
        count = await redis_client.delete(*keys)
        log.debug(f"Invalidated {count} keys matching pattern: {pattern}")
        return count
    except Exception as e:
        log.error(f"Error invalidating keys with pattern {pattern}: {e}")
        return 0

# Task queue operations
async def enqueue_task(queue_name: str, task_id: str, payload: Dict[str, Any]) -> bool:
    """Add task to queue."""
    redis_client = await get_redis()
    
    try:
        serialized = _json_serialize(payload)
        await redis_client.lpush(f"queue:{queue_name}", serialized)
        await redis_client.hset(f"tasks:{queue_name}", task_id, "pending")
        log.info(f"Enqueued task {task_id} to queue {queue_name}")
        return True
    except Exception as e:
        log.error(f"Error enqueueing task {task_id} to {queue_name}: {e}")
        return False

async def mark_task_complete(queue_name: str, task_id: str, result: Optional[Dict[str, Any]] = None) -> bool:
    """Mark task as complete with optional result."""
    redis_client = await get_redis()
    
    try:
        # Store result if provided
        if result:
            await redis_client.hset(
                f"results:{queue_name}", 
                task_id, 
                _json_serialize(result)
            )
        
        # Mark task as complete
        await redis_client.hset(f"tasks:{queue_name}", task_id, "complete")
        await redis_client.expire(f"tasks:{queue_name}", 86400)  # Expire after 24 hours
        
        log.info(f"Marked task {task_id} as complete in queue {queue_name}")
        return True
    except Exception as e:
        log.error(f"Error marking task {task_id} as complete: {e}")
        return False

async def get_task_status(queue_name: str, task_id: str) -> Optional[str]:
    """Get status of a task."""
    redis_client = await get_redis()
    
    try:
        status = await redis_client.hget(f"tasks:{queue_name}", task_id)
        return status
    except Exception as e:
        log.error(f"Error getting status for task {task_id}: {e}")
        return None

async def get_task_result(queue_name: str, task_id: str) -> Optional[Dict[str, Any]]:
    """Get result of a completed task."""
    redis_client = await get_redis()
    
    try:
        data = await redis_client.hget(f"results:{queue_name}", task_id)
        if not data:
            return None
            
        return _json_deserialize(data)
    except Exception as e:
        log.error(f"Error getting result for task {task_id}: {e}")
        return None

# Rate limiting
async def check_rate_limit(key: str, limit: int, window: int = 60) -> bool:
    """Check if rate limit is exceeded."""
    redis_client = await get_redis()
    
    try:
        current = await redis_client.get(key)
        if current and int(current) >= limit:
            return False
            
        pipe = redis_client.pipeline()
        pipe.incr(key)
        pipe.expire(key, window)
        await pipe.execute()
        
        return True
    except Exception as e:
        log.error(f"Error checking rate limit for {key}: {e}")
        return True  # Default to allowing on error

# Stream processing for real-time updates
async def add_to_stream(stream: str, data: Dict[str, Any], max_len: int = 1000) -> str:
    """Add event to Redis stream."""
    redis_client = await get_redis()
    
    try:
        # Convert dict values to strings (Redis streams requirement)
        entry = {k: _json_serialize(v) for k, v in data.items()}
        
        # Add to stream with automatic ID generation
        event_id = await redis_client.xadd(
            stream, 
            entry,
            maxlen=max_len,
            approximate=True
        )
        
        log.debug(f"Added event {event_id} to stream {stream}")
        return event_id
    except Exception as e:
        log.error(f"Error adding to stream {stream}: {e}")
        raise 