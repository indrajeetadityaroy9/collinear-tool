import os
import pytest
import httpx

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000/api")
EMAIL = os.environ.get("EMAIL", "testuser@example.com")
PASSWORD = os.environ.get("PASSWORD", "testpassword123")

@pytest.fixture(scope="session")
def client():
    with httpx.Client() as c:
        yield c

@pytest.fixture(scope="session")
def user_token(client):
    # Register (ignore if already registered)
    resp = client.post(f"{BASE_URL}/auth/register", json={"email": EMAIL, "password": PASSWORD})
    assert resp.status_code in (201, 400)
    # Login
    resp = client.post(f"{BASE_URL}/auth/login", json={"email": EMAIL, "password": PASSWORD})
    assert resp.status_code == 200
    return resp.json()["access_token"]

@pytest.fixture
def auth_headers(user_token):
    return {"Authorization": f"Bearer {user_token}"} 