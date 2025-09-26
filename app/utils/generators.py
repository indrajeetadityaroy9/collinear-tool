import json
import logging
from app.integrations.redis_client import get_redis_sync

log = logging.getLogger(__name__)


def dataset_generator(hash_key='hf:datasets:all:hash', batch_size=100):
    redis_client = get_redis_sync()
    if not redis_client:
        return
    cursor = '0'
    processed = 0
    while True:
        cursor, batch = redis_client.hscan(hash_key, cursor=cursor, count=batch_size)
        for dataset_id, dataset_json in batch.items():
            try:
                dataset = json.loads(dataset_json)
                yield dataset
                processed += 1
            except json.JSONDecodeError:
                log.warning(f"Failed to parse dataset JSON for ID: {dataset_id}")
                continue
        if cursor == '0':
            break
    log.info(f"Generated {processed} datasets")


def filtered_dataset_generator(filter_func, hash_key='hf:datasets:all:hash', batch_size=100):
    for dataset in dataset_generator(hash_key, batch_size):
        if filter_func(dataset):
            yield dataset


def paginated_generator(generator, offset=0, limit=10):
    for _ in range(offset):
        try:
            next(generator)
        except StopIteration:
            return
    count = 0
    for item in generator:
        if count >= limit:
            break
        yield item
        count += 1


async def async_dataset_generator(hash_key='hf:datasets:all:hash', batch_size=100):
    from app.integrations.redis_client import get_redis
    redis_client = await get_redis()
    if not redis_client:
        return
    cursor = b'0'
    processed = 0
    while True:
        cursor, batch = await redis_client.hscan(hash_key, cursor=cursor, count=batch_size)
        for dataset_id, dataset_json in batch.items():
            try:
                if isinstance(dataset_json, bytes):
                    dataset_json = dataset_json.decode('utf-8')
                dataset = json.loads(dataset_json)
                yield dataset
                processed += 1
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.warning(f"Failed to parse dataset JSON for ID: {dataset_id}: {e}")
                continue
        if cursor == b'0' or cursor == '0':
            break
    log.info(f"Async generated {processed} datasets")


def chunked_list_generator(items, chunk_size=50):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


class StreamingJSONResponse:
    def __init__(self, generator, total=None):
        self.generator = generator
        self.total = total
    def stream(self):
        yield '{"items":['
        first = True
        count = 0
        for item in self.generator:
            if not first:
                yield ','
            else:
                first = False
            yield json.dumps(item)
            count += 1
        yield ']'
        if self.total is not None:
            yield f',"total":{self.total}'
        yield f',"count":{count}'
        yield '}'


def transform_generator(generator, transform_func):
    for item in generator:
        try:
            transformed = transform_func(item)
            if transformed is not None:
                yield transformed
        except Exception as exc:
            log.warning(f"Failed to transform item: {exc}")
            continue


def batch_processor_generator(items, process_func, batch_size=10):
    for chunk in chunked_list_generator(items, batch_size):
        try:
            results = process_func(chunk)
            yield results
        except Exception as exc:
            log.error(f"Failed to process batch: {exc}")
            yield []