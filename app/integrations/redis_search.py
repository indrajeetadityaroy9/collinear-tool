import json
import logging
from app.integrations.redis_client import get_redis_sync, _json_deserialize

log = logging.getLogger(__name__)


def hscan_search_datasets(search_term, limit=10, offset=0, hash_key='hf:datasets:all:hash'):
    redis_client = get_redis_sync()
    if not redis_client:
        return ([], 0)
    search_lower = search_term.lower()
    matching_items = []
    total_matched = 0
    items_to_skip = offset
    cursor = '0'
    batch_size = 100
    try:
        while True:
            cursor, batch_data = redis_client.hscan(
                hash_key,
                cursor=cursor,
                count=batch_size
            )
            for dataset_id, dataset_json in batch_data.items():
                try:
                    dataset = json.loads(dataset_json)
                    id_match = search_lower in (dataset.get('id') or '').lower()
                    desc_match = search_lower in str(dataset.get('description') or '').lower()
                    name_match = search_lower in (dataset.get('name') or '').lower()
                    if id_match or desc_match or name_match:
                        total_matched += 1
                        if items_to_skip > 0:
                            items_to_skip -= 1
                            continue
                        if len(matching_items) < limit:
                            matching_items.append(dataset)
                except json.JSONDecodeError:
                    log.warning(f'Failed to parse dataset JSON for ID: {dataset_id}')
                    continue
            if cursor == '0' or (len(matching_items) >= limit and total_matched > offset + limit):
                break
        return (matching_items, total_matched)
    except Exception as exc:
        log.error(f'Error during HSCAN search: {exc}')
        return ([], 0)


def get_sorted_datasets_efficient(sort_by, sort_order='desc', limit=10, offset=0, zset_key='hf:datasets:all:zset', hash_key='hf:datasets:all:hash'):
    redis_client = get_redis_sync()
    if not redis_client:
        return ([], 0)
    try:
        sort_zset_key = f'{zset_key}:by_{sort_by}'
        if redis_client.exists(sort_zset_key):
            total = redis_client.zcard(sort_zset_key)
            if sort_order == 'desc':
                dataset_ids = redis_client.zrevrange(
                    sort_zset_key,
                    offset,
                    offset + limit - 1
                )
            else:
                dataset_ids = redis_client.zrange(
                    sort_zset_key,
                    offset,
                    offset + limit - 1
                )
            if dataset_ids:
                dataset_jsons = redis_client.hmget(hash_key, dataset_ids)
                datasets = []
                for dataset_json in dataset_jsons:
                    if dataset_json:
                        try:
                            datasets.append(json.loads(dataset_json))
                        except json.JSONDecodeError:
                            continue
                return (datasets, total)
            return ([], total)
        else:
            log.info(f'No sorted set for {sort_by}, using in-memory sort')
            return _fallback_sort_datasets(
                redis_client, sort_by, sort_order, limit, offset, hash_key, zset_key
            )
    except Exception as exc:
        log.error(f'Error during sorted dataset retrieval: {exc}')
        return ([], 0)


def _fallback_sort_datasets(redis_client, sort_by, sort_order, limit, offset, hash_key, zset_key):
    from app.utils.generators import dataset_generator
    datasets = list(dataset_generator(hash_key, batch_size=500))
    def sort_key(item):
        value = item.get(sort_by)
        if value is None:
            return (2, '')
        if isinstance(value, (int, float)):
            return (0, value)
        return (1, str(value).lower())
    reverse = sort_order == 'desc'
    datasets.sort(key=sort_key, reverse=reverse)
    paginated = datasets[offset:offset + limit]
    return (paginated, len(datasets))


def create_sort_index(field, zset_key='hf:datasets:all:zset', hash_key='hf:datasets:all:hash'):
    redis_client = get_redis_sync()
    if not redis_client:
        return False
    sort_zset_key = f'{zset_key}:by_{field}'
    try:
        pipe = redis_client.pipeline()
        all_data = redis_client.hgetall(hash_key)
        for dataset_id, dataset_json in all_data.items():
            try:
                dataset = json.loads(dataset_json)
                value = dataset.get(field)
                if value is None:
                    score = -1
                elif isinstance(value, (int, float)):
                    score = float(value)
                else:
                    score = hash(str(value)) % 1000000
                pipe.zadd(sort_zset_key, {dataset_id: score})
            except json.JSONDecodeError:
                continue
        pipe.execute()
        redis_client.expire(sort_zset_key, 43200)
        log.info(f'Created sort index for field: {field}')
        return True
    except Exception as exc:
        log.error(f'Error creating sort index for {field}: {exc}')
        return False


def search_and_sort_datasets(search_term=None, sort_by=None, sort_order='desc', limit=10, offset=0):
    if search_term and sort_by:
        search_results, _ = hscan_search_datasets(
            search_term,
            limit=1000,
            offset=0
        )
        def sort_key(item):
            value = item.get(sort_by)
            if value is None:
                return (2, '')
            if isinstance(value, (int, float)):
                return (0, value)
            return (1, str(value).lower())
        reverse = sort_order == 'desc'
        search_results.sort(key=sort_key, reverse=reverse)
        paginated = search_results[offset:offset + limit]
        return (paginated, len(search_results))
    elif search_term:
        return hscan_search_datasets(search_term, limit, offset)
    elif sort_by:
        return get_sorted_datasets_efficient(sort_by, sort_order, limit, offset)
    else:
        redis_client = get_redis_sync()
        if not redis_client:
            return ([], 0)
        total = redis_client.zcard('hf:datasets:all:zset')
        dataset_ids = redis_client.zrange(
            'hf:datasets:all:zset',
            offset,
            offset + limit - 1
        )
        if dataset_ids:
            dataset_jsons = redis_client.hmget('hf:datasets:all:hash', dataset_ids)
            datasets = []
            for dataset_json in dataset_jsons:
                if dataset_json:
                    try:
                        datasets.append(json.loads(dataset_json))
                    except json.JSONDecodeError:
                        continue
            return (datasets, total)
        return ([], total)