import copy
import pytest
from fastapi.testclient import TestClient
from app.main import app

@pytest.fixture(autouse=True)
def stub_hf_and_redis(monkeypatch):
    sample_page = {'total': 1, 'current_page': 1, 'total_pages': 1, 'next_page': None, 'prev_page': None, 'items': [{'id': 'openbmb/Ultra-FineWeb', 'description': 'Synthetic entry'}], 'warming_up': False}

    async def fake_cache_get(key):
        if key == 'hf:datasets:meta':
            return {'last_update': '2024-06-01T00:00:00Z', 'total_items': 1, 'refreshing': False}
        return None

    def fake_get_page(limit, offset, search, sort_by, sort_order):
        if offset >= 10:
            emptied = copy.deepcopy(sample_page)
            emptied.update({'items': [], 'current_page': 0, 'next_page': None, 'prev_page': None})
            return (emptied, 200)
        return (copy.deepcopy(sample_page), 200)

    async def fake_get_commits(dataset_id):
        return [{'id': 'commit-1', 'title': 'Initial release', 'message': 'Initial release', 'author': {'name': 'tester', 'email': ''}, 'date': '2024-06-01T00:00:00Z'}]

    async def fake_get_files(dataset_id):
        return ['README.md', 'data.csv']

    async def fake_get_file_url(dataset_id, filename, revision=None):
        return f'https://example.com/{dataset_id}/{filename}'
    monkeypatch.setattr('app.api.routes.datasets.cache_get', fake_cache_get)
    monkeypatch.setattr('app.api.routes.datasets.get_datasets_page_from_cache', fake_get_page)
    monkeypatch.setattr('app.api.routes.datasets.get_dataset_commits_async', fake_get_commits)
    monkeypatch.setattr('app.api.routes.datasets.get_dataset_files_async', fake_get_files)
    monkeypatch.setattr('app.api.routes.datasets.get_file_url_async', fake_get_file_url)

@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)

def test_list_datasets_default(client):
    resp = client.get('/api/datasets')
    assert resp.status_code == 200
    data = resp.json()
    assert 'items' in data and isinstance(data['items'], list)
    assert data['total'] == 1
    assert data['warming_up'] is False

def test_list_datasets_offset_limit(client):
    resp = client.get('/api/datasets?offset=0&limit=2')
    assert resp.status_code == 200
    assert len(resp.json()['items']) <= 2

def test_list_datasets_large_offset(client):
    resp = client.get('/api/datasets?offset=100000&limit=2')
    assert resp.status_code == 200
    assert resp.json()['items'] == []

def test_list_datasets_negative_limit(client):
    resp = client.get('/api/datasets?limit=-1')
    assert resp.status_code == 422

def test_cache_status(client):
    resp = client.get('/api/datasets/cache-status')
    assert resp.status_code == 200
    data = resp.json()
    assert data['total_items'] == 1
    assert data['warming_up'] is False
    assert data['refreshing'] is False

def test_get_commits_valid(client):
    resp = client.get('/api/datasets/openbmb/Ultra-FineWeb/commits')
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)

def test_get_commits_invalid(monkeypatch, client):

    async def failing(dataset_id):
        raise RuntimeError('boom')
    monkeypatch.setattr('app.api.routes.datasets.get_dataset_commits_async', failing)
    resp = client.get('/api/datasets/invalid-dataset-id/commits')
    assert resp.status_code == 404

def test_list_files_valid(client):
    resp = client.get('/api/datasets/openbmb/Ultra-FineWeb/files')
    assert resp.status_code == 200
    assert resp.json() == ['README.md', 'data.csv']

def test_list_files_invalid(monkeypatch, client):

    async def failing(dataset_id):
        raise RuntimeError('missing')
    monkeypatch.setattr('app.api.routes.datasets.get_dataset_files_async', failing)
    resp = client.get('/api/datasets/invalid-dataset-id/files')
    assert resp.status_code == 404

def test_get_file_url_valid(client):
    resp = client.get('/api/datasets/openbmb/Ultra-FineWeb/file-url', params={'filename': 'README.md'})
    assert resp.status_code == 200
    assert resp.json()['download_url'].endswith('README.md')

def test_get_file_url_invalid_file(monkeypatch, client):

    async def failing(dataset_id, filename, revision=None):
        raise RuntimeError('not found')
    monkeypatch.setattr('app.api.routes.datasets.get_file_url_async', failing)
    resp = client.get('/api/datasets/openbmb/Ultra-FineWeb/file-url', params={'filename': 'not_a_real_file.txt'})
    assert resp.status_code == 500

def test_get_file_url_missing_filename(client):
    resp = client.get('/api/datasets/openbmb/Ultra-FineWeb/file-url')
    assert resp.status_code == 422
