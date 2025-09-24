from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_list_datasets_default():
    resp = client.get("/api/datasets")
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert isinstance(data["items"], list)
    assert "total" in data
    assert "warming_up" in data

def test_list_datasets_offset_limit():
    resp = client.get("/api/datasets?offset=0&limit=2")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data["items"], list)
    assert len(data["items"]) <= 2

def test_list_datasets_large_offset():
    resp = client.get("/api/datasets?offset=100000&limit=2")
    assert resp.status_code == 200
    data = resp.json()
    assert data["items"] == []
    assert data["warming_up"] in (True, False)

def test_list_datasets_negative_limit():
    resp = client.get("/api/datasets?limit=-1")
    assert resp.status_code == 422

def test_list_datasets_missing_params():
    resp = client.get("/api/datasets")
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert "total" in data
    assert "warming_up" in data


def test_cache_status():
    resp = client.get("/api/datasets/cache-status")
    assert resp.status_code == 200
    data = resp.json()
    assert "warming_up" in data
    assert "total_items" in data
    assert "last_update" in data
    assert "refreshing" in data


def test_get_commits_valid():
    resp = client.get("/api/datasets/openbmb/Ultra-FineWeb/commits")

    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        assert isinstance(resp.json(), list)

def test_get_commits_invalid():
    resp = client.get("/api/datasets/invalid-dataset-id/commits")
    assert resp.status_code in (404, 422)


def test_list_files_valid():
    resp = client.get("/api/datasets/openbmb/Ultra-FineWeb/files")
    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        assert isinstance(resp.json(), list)

def test_list_files_invalid():
    resp = client.get("/api/datasets/invalid-dataset-id/files")
    assert resp.status_code in (404, 422)


def test_get_file_url_valid():
    resp = client.get("/api/datasets/openbmb/Ultra-FineWeb/file-url", params={"filename": "README.md"})
    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        assert "download_url" in resp.json()

def test_get_file_url_invalid_file():
    resp = client.get("/api/datasets/openbmb/Ultra-FineWeb/file-url", params={"filename": "not_a_real_file.txt"})
    assert resp.status_code in (404, 200)

def test_get_file_url_missing_filename():
    resp = client.get("/api/datasets/openbmb/Ultra-FineWeb/file-url")
    assert resp.status_code in (404, 422)
