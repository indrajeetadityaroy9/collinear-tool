import hashlib
import json
import logging
from functools import wraps
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from app.integrations.redis_client import cache_get, cache_set

log = logging.getLogger(__name__)


def generate_cache_key(prefix, request):
    path = request.url.path
    query_params = sorted(request.query_params.items())
    key_parts = [prefix, path]
    if query_params:
        query_str = "&".join([f"{k}={v}" for k, v in query_params])
        key_parts.append(query_str)
    return ":".join(key_parts)


def generate_etag(content):
    content_str = json.dumps(content, sort_keys=True)
    hash_obj = hashlib.md5(content_str.encode())
    return f'"{hash_obj.hexdigest()}"'


def cache_response(ttl=300, key_prefix="api:cache", include_etag=True):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            if not request:
                request = kwargs.get('request')
            if not request:
                return await func(*args, **kwargs)
            cache_key = generate_cache_key(key_prefix, request)
            if include_etag:
                if_none_match = request.headers.get('if-none-match')
                if if_none_match:
                    cached_data = await cache_get(cache_key)
                    if cached_data:
                        cached_content = cached_data.get('content')
                        cached_etag = cached_data.get('etag')
                        if cached_etag and cached_etag == if_none_match:
                            return Response(
                                status_code=304,
                                headers={'etag': cached_etag}
                            )
            cached_data = await cache_get(cache_key)
            if cached_data:
                log.debug(f"Cache hit for key: {cache_key}")
                content = cached_data.get('content')
                etag = cached_data.get('etag')
                headers = {}
                if include_etag and etag:
                    headers['etag'] = etag
                headers['x-cache'] = 'HIT'
                return JSONResponse(
                    content=content,
                    headers=headers
                )
            log.debug(f"Cache miss for key: {cache_key}")
            result = await func(*args, **kwargs)
            if isinstance(result, dict):
                content = result
            elif hasattr(result, 'dict'):
                content = result.dict()
            else:
                return result
            etag = None
            if include_etag:
                etag = generate_etag(content)
            cache_data = {
                'content': content,
                'etag': etag
            }
            await cache_set(cache_key, cache_data, expire=ttl)
            headers = {}
            if include_etag and etag:
                headers['etag'] = etag
            headers['x-cache'] = 'MISS'
            return JSONResponse(
                content=content,
                headers=headers
            )
        return wrapper
    return decorator


def invalidate_cache_pattern(pattern):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            from app.integrations.redis_client import cache_invalidate_pattern
            try:
                count = await cache_invalidate_pattern(pattern)
                log.info(f"Invalidated {count} cache entries matching pattern: {pattern}")
            except Exception as exc:
                log.error(f"Failed to invalidate cache pattern {pattern}: {exc}")
            return result
        return wrapper
    return decorator


def cache_status_endpoint(ttl=30):
    return cache_response(
        ttl=ttl,
        key_prefix="api:cache:status",
        include_etag=False
    )