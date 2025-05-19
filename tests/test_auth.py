import os
import pytest

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000/api")
EMAIL = os.environ.get("EMAIL", "testuser@example.com")
PASSWORD = os.environ.get("PASSWORD", "testpassword123")


def test_register(client):
    resp = client.post(f"{BASE_URL}/auth/register", json={"email": EMAIL, "password": PASSWORD})
    assert resp.status_code in (201, 400, 409)


def test_login(client):
    resp = client.post(f"{BASE_URL}/auth/login", json={"email": EMAIL, "password": PASSWORD})
    assert resp.status_code == 200
    assert "access_token" in resp.json()


def test_invalid_refresh_token(client):
    resp = client.post(f"{BASE_URL}/users/refresh", json={"refresh_token": "invalidtoken"})
    assert resp.status_code == 401 