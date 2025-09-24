import os
import requests

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000/api")


def test_list_datasets_http():
    resp = requests.get(f"{BASE_URL}/datasets")
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert "total" in data
    assert "warming_up" in data

def test_list_datasets_offset_limit_http():
    resp = requests.get(f"{BASE_URL}/datasets?offset=0&limit=3")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data["items"], list)
    assert len(data["items"]) <= 3

def test_list_datasets_large_offset_http():
    resp = requests.get(f"{BASE_URL}/datasets?offset=99999&limit=2")
    assert resp.status_code == 200
    data = resp.json()
    assert data["items"] == []
    assert "warming_up" in data

def test_list_datasets_invalid_limit_http():
    resp = requests.get(f"{BASE_URL}/datasets?limit=-5")
    assert resp.status_code == 422


def test_cache_status_http():
    resp = requests.get(f"{BASE_URL}/datasets/cache-status")
    assert resp.status_code == 200
    data = resp.json()
    assert "warming_up" in data
    assert "total_items" in data
    assert "last_update" in data
    assert "refreshing" in data


def test_commits_valid_http():
    resp = requests.get(f"{BASE_URL}/datasets/openbmb/Ultra-FineWeb/commits")
    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        assert isinstance(resp.json(), list)

def test_commits_invalid_http():
    resp = requests.get(f"{BASE_URL}/datasets/invalid-dataset-id/commits")
    assert resp.status_code in (404, 422)


def test_files_valid_http():
    resp = requests.get(f"{BASE_URL}/datasets/openbmb/Ultra-FineWeb/files")
    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        assert isinstance(resp.json(), list)

def test_files_invalid_http():
    resp = requests.get(f"{BASE_URL}/datasets/invalid-dataset-id/files")
    assert resp.status_code in (404, 422)


def test_file_url_valid_http():
    resp = requests.get(f"{BASE_URL}/datasets/openbmb/Ultra-FineWeb/file-url", params={"filename": "README.md"})
    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        assert "download_url" in resp.json()

def test_file_url_invalid_file_http():
    resp = requests.get(f"{BASE_URL}/datasets/openbmb/Ultra-FineWeb/file-url", params={"filename": "not_a_real_file.txt"})
    assert resp.status_code in (404, 200)

def test_file_url_missing_filename_http():
    resp = requests.get(f"{BASE_URL}/datasets/openbmb/Ultra-FineWeb/file-url")
    assert resp.status_code in (404, 422)
