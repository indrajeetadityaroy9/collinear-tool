import os
import pytest

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000/api")

def test_health_supabase(client):
    resp = client.get(f"{BASE_URL}/health/supabase")
    assert resp.status_code in (200, 503)
    assert "status" in resp.json() or resp.status_code == 503

def test_health_live(client):
    resp = client.get(f"{BASE_URL}/health/live")
    assert resp.status_code == 200
    assert resp.json()["status"] == "alive" 