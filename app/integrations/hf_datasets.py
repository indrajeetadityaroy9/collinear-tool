import asyncio
import json
import logging
from datetime import datetime, timezone
import httpx
from huggingface_hub import HfApi
from app.core.config import settings
from app.integrations.redis_client import get_redis_sync

log = logging.getLogger(__name__)
HF_API_URL = 'https://huggingface.co/api/datasets'
REDIS_ZSET_KEY = 'hf:datasets:all:zset'
REDIS_HASH_KEY = 'hf:datasets:all:hash'
REDIS_META_KEY = 'hf:datasets:meta'


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _resolve_hf_token(explicit_token=None):
    token = settings.resolve_hf_token(explicit_token)
    if not token:
        raise RuntimeError('Hugging Face API token is not configured.')
    return token


def _safe_get_redis():
    try:
        return get_redis_sync()
    except Exception as exc:
        log.warning('Redis unavailable: %s', exc)
        return None


def _update_cache_meta(redis_client, total_items, *, refreshing):
    meta = {'last_update': datetime.now(timezone.utc).isoformat(), 'total_items': total_items, 'refreshing': refreshing}
    redis_client.set(REDIS_META_KEY, json.dumps(meta))


def _normalize_cached_item(item):
    assessment = item.get('impact_assessment')
    if isinstance(assessment, str):
        try:
            item['impact_assessment'] = json.loads(assessment)
        except json.JSONDecodeError:
            pass
    tags = item.get('tags')
    if isinstance(tags, str):
        try:
            item['tags'] = json.loads(tags)
        except json.JSONDecodeError:
            item['tags'] = [tags] if tags else []
    for numeric_field in ('size_bytes', 'downloads', 'likes'):
        value = item.get(numeric_field)
        if isinstance(value, str):
            if value.lower() == 'null':
                item[numeric_field] = None
                continue
            try:
                item[numeric_field] = int(float(value))
            except (ValueError, TypeError):
                item[numeric_field] = None
    return item


def _load_all_cached_items(redis_client):
    raw_items = redis_client.hgetall(REDIS_HASH_KEY)
    if raw_items:
        return [_normalize_cached_item(json.loads(value)) for value in raw_items.values()]
    return []


def determine_impact_level_by_criteria(size_bytes, downloads=None, likes=None):
    try:
        size = int(size_bytes) if size_bytes not in (None, 'null') else 0
    except Exception:
        size = 0
    if size >= 10 * 1024 * 1024 * 1024:
        return ('high', 'large_size')
    if size >= 1 * 1024 * 1024 * 1024:
        return ('medium', 'medium_size')
    if size >= 100 * 1024 * 1024:
        return ('low', 'small_size')
    if downloads is not None:
        try:
            downloads = int(downloads)
            if downloads >= 100000:
                return ('high', 'downloads')
            if downloads >= 10000:
                return ('medium', 'downloads')
            if downloads >= 1000:
                return ('low', 'downloads')
        except Exception:
            pass
    if likes is not None:
        try:
            likes = int(likes)
            if likes >= 1000:
                return ('high', 'likes')
            if likes >= 100:
                return ('medium', 'likes')
            if likes >= 10:
                return ('low', 'likes')
        except Exception:
            pass
    return ('not_available', 'size_and_downloads_and_likes_unknown')


async def _fetch_size(client, dataset_id):
    url = f'https://datasets-server.huggingface.co/size?dataset={dataset_id}'
    try:
        resp = await client.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('size', {}).get('dataset', {}).get('num_bytes_original_files')
    except Exception as exc:
        log.warning('Could not fetch size for %s: %s', dataset_id, exc)
    return None


async def _fetch_sizes(dataset_ids, token=None):
    results = {}
    dataset_ids = [dataset_id for dataset_id in dataset_ids if dataset_id]
    if not dataset_ids:
        return results
    from app.utils.http_client import get_hf_client
    client = await get_hf_client(token)
    tasks = [asyncio.create_task(_fetch_size(client, ds_id)) for ds_id in dataset_ids]
    for (dataset_id, task) in zip(dataset_ids, tasks):
        results[dataset_id] = await task
    return results


def _build_cached_item(dataset_id, raw, size_bytes, downloads, likes):
    (impact_level, assessment_method) = determine_impact_level_by_criteria(size_bytes, downloads, likes)
    metrics = {
        'size_bytes': size_bytes,
        'downloads': downloads,
        'likes': likes,
    }
    thresholds = {
        'size_bytes': {
            'low': str(100 * 1024 * 1024),
            'medium': str(1 * 1024 * 1024 * 1024),
            'high': str(10 * 1024 * 1024 * 1024),
        }
    }
    impact_assessment = {
        'dataset_id': dataset_id,
        'impact_level': impact_level,
        'assessment_method': assessment_method,
        'metrics': metrics,
        'thresholds': thresholds,
    }
    item = {
        'id': dataset_id,
        'name': raw.get('name') or dataset_id,
        'description': raw.get('description'),
        'size_bytes': size_bytes,
        'impact_level': impact_level,
        'downloads': downloads,
        'likes': likes,
        'tags': raw.get('tags') or [],
        'card_data': raw.get('cardData'),
        'impact_assessment': impact_assessment,
    }
    for (key, value) in raw.items():
        item.setdefault(key, value)
    return item


async def process_datasets_page(offset, limit):
    log.info('[process_datasets_page] ENTRY: offset=%s limit=%s', offset, limit)
    token = _resolve_hf_token()
    headers = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0 (compatible; CollinearTool/1.0; +https://yourdomain.com)'}
    params = {'limit': limit, 'offset': offset, 'full': 'True'}
    async with httpx.AsyncClient() as client:
        response = await client.get(HF_API_URL, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        page_items = response.json()
    log.info('[process_datasets_page] Received %s datasets at offset %s', len(page_items), offset)
    dataset_ids = [item.get('id') for item in page_items]
    size_map = await _fetch_sizes(dataset_ids, token=token) if dataset_ids else {}
    redis_client = _safe_get_redis()
    if redis_client is None:
        raise RuntimeError('Redis connection unavailable while processing datasets page')
    pipe = redis_client.pipeline()
    stored = 0
    for (index, dataset) in enumerate(page_items):
        dataset_id = dataset.get('id')
        if not dataset_id:
            continue
        size_bytes = size_map.get(dataset_id)
        downloads = dataset.get('downloads')
        likes = dataset.get('likes')
        cached_item = _build_cached_item(dataset_id, dataset, size_bytes, downloads, likes)
        pipe.zadd(REDIS_ZSET_KEY, {dataset_id: offset + index})
        if downloads is not None:
            pipe.zadd(f'{REDIS_ZSET_KEY}:by_downloads', {dataset_id: float(downloads) if downloads else 0})
        if likes is not None:
            pipe.zadd(f'{REDIS_ZSET_KEY}:by_likes', {dataset_id: float(likes) if likes else 0})
        if size_bytes is not None:
            pipe.zadd(f'{REDIS_ZSET_KEY}:by_size_bytes', {dataset_id: float(size_bytes) if size_bytes else 0})
        pipe.hset(REDIS_HASH_KEY, dataset_id, json.dumps(cached_item, cls=EnhancedJSONEncoder))
        stored += 1
    pipe.execute()
    total_items = redis_client.zcard(REDIS_ZSET_KEY)
    _update_cache_meta(redis_client, total_items, refreshing=False)
    log.info('[process_datasets_page] EXIT: Cached %s datasets at offset %s', stored, offset)
    return stored


async def refresh_datasets_cache():
    log.info('[refresh_datasets_cache] Orchestrating dataset fetch tasks using direct HF API calls.')
    token = _resolve_hf_token()
    headers = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0 (compatible; CollinearTool/1.0; +https://yourdomain.com)'}
    async with httpx.AsyncClient() as client:
        response = await client.get(HF_API_URL, headers=headers, params={'limit': 1, 'offset': 0}, timeout=120)
        response.raise_for_status()
        total_str = response.headers.get('X-Total-Count')
    if not total_str:
        raise ValueError("'X-Total-Count' header missing from Hugging Face API response.")
    total = int(total_str)
    limit = 500
    num_pages = (total + limit - 1) // limit
    redis_client = _safe_get_redis()
    if redis_client is not None:
        current_total = redis_client.zcard(REDIS_ZSET_KEY)
        _update_cache_meta(redis_client, current_total, refreshing=True)
    from app.tasks.dataset_tasks import fetch_datasets_page
    from celery import group
    tasks = [fetch_datasets_page.s(page * limit, limit) for page in range(num_pages)]
    if tasks:
        group(tasks).apply_async()
        log.info('[refresh_datasets_cache] Enqueued %s fetch tasks.', len(tasks))


async def get_dataset_commits_async(dataset_id, limit=20):
    from app.integrations.hf_client import AsyncHuggingFaceClient
    log.info('[get_dataset_commits_async] Fetching commits for dataset_id=%s', dataset_id)
    token = _resolve_hf_token()
    client = AsyncHuggingFaceClient(token=token)
    try:
        return await client.list_repo_commits(
            repo_id=dataset_id,
            repo_type='dataset',
            limit=limit
        )
    except Exception as exc:
        log.error('[get_dataset_commits_async] Error fetching commits for %s: %s', dataset_id, exc, exc_info=True)
        raise


async def get_dataset_files_async(dataset_id):
    from app.integrations.hf_client import AsyncHuggingFaceClient
    token = _resolve_hf_token()
    client = AsyncHuggingFaceClient(token=token)
    return await client.list_repo_files(repo_id=dataset_id, repo_type='dataset')


async def get_file_url_async(dataset_id, filename, revision=None):
    from app.integrations.hf_client import AsyncHuggingFaceClient
    token = _resolve_hf_token()
    client = AsyncHuggingFaceClient(token=token)
    return client.get_file_url(
        repo_id=dataset_id,
        filename=filename,
        repo_type='dataset',
        revision=revision
    )


async def fetch_size(session, dataset_id, token=None):
    return await _fetch_size(session, dataset_id, token)


async def fetch_all_sizes(dataset_ids, token=None, batch_size=50):
    results = {}
    dataset_ids = list(dataset_ids)
    if not dataset_ids:
        return results
    async with httpx.AsyncClient() as session:
        for i in range(0, len(dataset_ids), batch_size):
            batch = dataset_ids[i:i + batch_size]
            tasks = [fetch_size(session, ds_id, token) for ds_id in batch]
            batch_results = await asyncio.gather(*tasks)
            for (ds_id, size) in zip(batch, batch_results):
                results[ds_id] = size
    return results


def fetch_and_cache_all_datasets(token=None):
    resolved_token = _resolve_hf_token(token)
    api = HfApi(token=resolved_token)
    log.info('Fetching all datasets from Hugging Face Hub...')
    all_datasets = list(api.list_datasets())
    dataset_ids = [dataset.id for dataset in all_datasets]
    sizes = asyncio.run(fetch_all_sizes(dataset_ids, token=resolved_token, batch_size=50)) if dataset_ids else {}
    redis_client = _safe_get_redis()
    if redis_client is None:
        raise RuntimeError('Redis connection unavailable while caching datasets')
    pipe = redis_client.pipeline()
    pipe.delete(REDIS_ZSET_KEY, REDIS_HASH_KEY)
    pipe.delete(f'{REDIS_ZSET_KEY}:by_downloads')
    pipe.delete(f'{REDIS_ZSET_KEY}:by_likes')
    pipe.delete(f'{REDIS_ZSET_KEY}:by_size_bytes')
    stored_count = 0
    for (index, dataset) in enumerate(all_datasets):
        raw_data = json.loads(json.dumps(dataset.__dict__, cls=EnhancedJSONEncoder))
        dataset_id = dataset.id
        size_bytes = sizes.get(dataset_id)
        downloads = raw_data.get('downloads')
        likes = raw_data.get('likes')
        cached_item = _build_cached_item(dataset_id, raw_data, size_bytes, downloads, likes)
        pipe.zadd(REDIS_ZSET_KEY, {dataset_id: index})
        if downloads is not None:
            pipe.zadd(f'{REDIS_ZSET_KEY}:by_downloads', {dataset_id: float(downloads) if downloads else 0})
        if likes is not None:
            pipe.zadd(f'{REDIS_ZSET_KEY}:by_likes', {dataset_id: float(likes) if likes else 0})
        if size_bytes is not None:
            pipe.zadd(f'{REDIS_ZSET_KEY}:by_size_bytes', {dataset_id: float(size_bytes) if size_bytes else 0})
        pipe.hset(REDIS_HASH_KEY, dataset_id, json.dumps(cached_item, cls=EnhancedJSONEncoder))
        stored_count += 1
    pipe.execute()
    _update_cache_meta(redis_client, stored_count, refreshing=False)
    log.info('Cached %s datasets in Redis (Hash+ZSet)', stored_count)
    return stored_count


def _sort_value(value):
    if value is None:
        return (2, '')
    if isinstance(value, (int, float)):
        return (0, value)
    if isinstance(value, str):
        stripped = value.strip()
        try:
            numeric = float(stripped)
            return (0, numeric)
        except ValueError:
            return (1, stripped.lower())
    return (2, str(value).lower())


def _build_paginated_response(items, total, limit, offset):
    if total == 0:
        return {'total': 0, 'current_page': 0, 'total_pages': 0, 'next_page': None, 'prev_page': None, 'items': []}
    total_pages = (total + limit - 1) // limit if limit else 0
    if offset >= total:
        current_page = total_pages if total_pages else 0
        page_items = []
    else:
        current_page = offset // limit + 1 if limit else 1
        page_items = items[offset:offset + limit]
    next_page = current_page + 1 if current_page < total_pages else None
    prev_page = current_page - 1 if current_page > 1 else None
    return {'total': total, 'current_page': current_page, 'total_pages': total_pages, 'next_page': next_page, 'prev_page': prev_page, 'items': page_items}


def get_datasets_page_from_cache(limit, offset, search=None, sort_by=None, sort_order='desc'):
    redis_client = _safe_get_redis()
    if redis_client is None:
        response = _build_paginated_response([], 0, limit, offset)
        response['warming_up'] = True
        return (response, 200)
    total = redis_client.zcard(REDIS_ZSET_KEY)
    if total == 0:
        response = _build_paginated_response([], 0, limit, offset)
        response['warming_up'] = True
        return (response, 200)
    if search or sort_by:
        from app.integrations.redis_search import search_and_sort_datasets
        items, total_filtered = search_and_sort_datasets(
            search_term=search,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset
        )
        items = [_normalize_cached_item(item) for item in items]
        response = _build_paginated_response(items, total_filtered, limit, offset)
        response['warming_up'] = False
        return (response, 200)
    ids = redis_client.zrange(REDIS_ZSET_KEY, offset, offset + limit - 1)
    if ids:
        raw_items = redis_client.hmget(REDIS_HASH_KEY, ids)
        page_items = [_normalize_cached_item(json.loads(item)) for item in raw_items if item]
    else:
        page_items = []
    response = _build_paginated_response(page_items, total, limit, offset)
    response['warming_up'] = False
    return (response, 200)