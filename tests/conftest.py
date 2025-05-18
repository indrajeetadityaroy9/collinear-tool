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
    return "eyJhbGciOiJIUzI1NiIsImtpZCI6InRlc3Qta2V5IiwidHlwIjoiSldUIn0.eyJpc3MiOiJzdXBhYmFzZSIsInN1YiI6InRlc3QtdXNlci1pZCIsImVtYWlsIjoidGVzdEBleGFtcGxlLmNvbSJ9.test-signature"


@pytest.fixture
def auth_headers(user_token):
    return {"Authorization": f"Bearer {user_token}"} 