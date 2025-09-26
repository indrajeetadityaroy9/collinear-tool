import json
import pytest
from app.integrations import hf_datasets


class InMemoryRedis:
    def __init__(self):
        self.zsets = {}
        self.hashes = {}
        self.strings = {}
    def zadd(self, key, mapping):
        bucket = self.zsets.setdefault(key, {})
        for (member, score) in mapping.items():
            bucket[member] = score
    def zcard(self, key):
        return len(self.zsets.get(key, {}))
    def zrange(self, key, start, end):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
        members = [member for (member, _) in items]
        slice_end = None if end == -1 else end + 1
        return members[start:slice_end]
    def hset(self, key, field, value=None):
        bucket = self.hashes.setdefault(key, {})
        if isinstance(field, dict):
            bucket.update(field)
        else:
            bucket[field] = value
    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))
    def hmget(self, key, fields):
        bucket = self.hashes.get(key, {})
        return [bucket.get(field) for field in fields]
    def set(self, key, value):
        self.strings[key] = value
    def get(self, key):
        return self.strings.get(key)
    def delete(self, *keys):
        count = 0
        for key in keys:
            for container in (self.zsets, self.hashes, self.strings):
                if key in container:
                    del container[key]
                    count += 1
        return count


@pytest.fixture
def fake_redis(monkeypatch):
    redis_instance = InMemoryRedis()
    monkeypatch.setattr('app.integrations.hf_datasets.get_redis_sync', lambda : redis_instance)
    return redis_instance


def test_get_datasets_page_from_cache_returns_items(fake_redis):
    dataset = {'id': 'dataset-1', 'description': 'Test dataset', 'downloads': 10, 'likes': 1, 'tags': ['demo'], 'size_bytes': 1000}
    fake_redis.zadd(hf_datasets.REDIS_ZSET_KEY, {dataset['id']: 0})
    fake_redis.hset(hf_datasets.REDIS_HASH_KEY, dataset['id'], json.dumps(dataset))
    fake_redis.zadd(f'{hf_datasets.REDIS_ZSET_KEY}:by_downloads', {dataset['id']: 10})
    fake_redis.zadd(f'{hf_datasets.REDIS_ZSET_KEY}:by_likes', {dataset['id']: 1})
    fake_redis.zadd(f'{hf_datasets.REDIS_ZSET_KEY}:by_size_bytes', {dataset['id']: 1000})
    (result, status) = hf_datasets.get_datasets_page_from_cache(limit=5, offset=0)
    assert status == 200
    assert result['total'] == 1
    assert result['items'][0]['id'] == 'dataset-1'
    assert result['warming_up'] is False


def test_get_datasets_page_from_cache_filters_search(fake_redis):
    first = {'id': 'dataset-alpha', 'description': 'Alpha'}
    second = {'id': 'dataset-beta', 'description': 'Beta'}
    fake_redis.zadd(hf_datasets.REDIS_ZSET_KEY, {first['id']: 0, second['id']: 1})
    fake_redis.hset(hf_datasets.REDIS_HASH_KEY, first['id'], json.dumps(first))
    fake_redis.hset(hf_datasets.REDIS_HASH_KEY, second['id'], json.dumps(second))
    (result, status) = hf_datasets.get_datasets_page_from_cache(limit=5, offset=0, search='beta')
    assert status == 200
    assert len(result['items']) == 1
    assert result['items'][0]['id'] == 'dataset-beta'


def test_get_datasets_page_from_cache_warming_up(fake_redis):
    (result, status) = hf_datasets.get_datasets_page_from_cache(limit=5, offset=0)
    assert status == 200
    assert result['items'] == []
    assert result['warming_up'] is True


def test_determine_impact_level_by_size():
    (level, reason) = hf_datasets.determine_impact_level_by_criteria(15 * 1024 * 1024 * 1024)
    assert level == 'high'
    assert reason == 'large_size'


@pytest.mark.asyncio
async def test_fetch_all_sizes_pairs_ids(monkeypatch):
    class MockClient:
        async def get(self, url, timeout=None):
            class Response:
                status_code = 200
                def json(self):
                    return {'size': {'dataset': {'num_bytes_original_files': 1000}}}
            return Response()

    async def mock_get_hf_client(token=None):
        return MockClient()
    monkeypatch.setattr('app.utils.http_client.get_hf_client', mock_get_hf_client)
    result = await hf_datasets._fetch_sizes(['dataset1', 'dataset2'], token='token')
    assert len(result) == 2
    assert result['dataset1'] == 1000
    assert result['dataset2'] == 1000