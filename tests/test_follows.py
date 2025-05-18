import os
import pytest

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000/api")
DATASET_ID = "openbmb/Ultra-FineWeb"


def test_follow_and_unfollow(client, auth_headers):
    # Follow
    resp = client.post(f"{BASE_URL}/follows", json={"dataset_id": DATASET_ID}, headers=auth_headers)
    if resp.status_code != 204:
        print("Follow error:", resp.status_code, resp.text)
        # Accept 400 only if error is 'User already followed'
        assert resp.status_code == 400
        detail = resp.json().get("detail", "")
        assert "already" in detail.lower() or "duplicate" in detail.lower()
    
    # List follows
    resp = client.get(f"{BASE_URL}/follows", headers=auth_headers)
    print("Follows after follow:", resp.json())
    assert resp.status_code == 200
    assert any(f["dataset_id"] == DATASET_ID for f in resp.json())

    # Unfollow
    resp = client.delete(f"{BASE_URL}/follows/{DATASET_ID}", headers=auth_headers)
    if resp.status_code != 204:
        print("Unfollow error:", resp.status_code, resp.text)
        assert resp.status_code == 404
    
    # List follows again
    resp = client.get(f"{BASE_URL}/follows", headers=auth_headers)
    print("Follows after unfollow:", resp.json())
    assert resp.status_code == 200
    assert not any(f["dataset_id"] == DATASET_ID for f in resp.json()) 