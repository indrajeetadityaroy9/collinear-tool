import os
import pytest

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000/api")


def test_list_datasets(client):
    resp = client.get(f"{BASE_URL}/datasets")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_commit_history(client):
    resp = client.get(f"{BASE_URL}/datasets/openbmb/Ultra-FineWeb/commits")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_list_files(client):
    resp = client.get(f"{BASE_URL}/datasets/openbmb/Ultra-FineWeb/files")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_file_url(client):
    # Try to get a file URL for a likely file in the dataset
    dataset_id = "openbmb/Ultra-FineWeb"
    filename = "README.md"  # Adjust if you know a real file
    resp = client.get(f"{BASE_URL}/datasets/{dataset_id}/file-url", params={"filename": filename})
    # Accept 200 (found) or 404 (not found)
    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        assert "download_url" in resp.json()


def test_follow_dataset_endpoint(client, auth_headers):
    dataset_id = "openbmb/Ultra-FineWeb"
    resp = client.post(f"{BASE_URL}/datasets/{dataset_id}/follow", headers=auth_headers)
    # Accept 204 (success) or 400 (already followed)
    assert resp.status_code in (204, 400)


def test_unfollow_dataset_endpoint(client, auth_headers):
    dataset_id = "openbmb/Ultra-FineWeb"
    resp = client.delete(f"{BASE_URL}/datasets/{dataset_id}/follow", headers=auth_headers)
    # Accept 204 (success) or 404 (not followed)
    assert resp.status_code in (204, 404) 