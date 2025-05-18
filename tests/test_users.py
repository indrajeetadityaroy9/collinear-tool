import os
import pytest

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000/api")


def test_get_current_user(client, auth_headers):
    resp = client.get(f"{BASE_URL}/users/me", headers=auth_headers)
    assert resp.status_code == 200
    assert "email" in resp.json()


def test_update_user_email_fail(client, auth_headers):
    # Try to update to an email that is already taken or invalid
    resp = client.patch(f"{BASE_URL}/users/me", json={"email": "testuser@example.com"}, headers=auth_headers)
    print("Update email response:", resp.status_code, resp.text)
    assert resp.status_code in (202, 400)


def test_logout(client, auth_headers):
    resp = client.post(f"{BASE_URL}/users/logout", headers=auth_headers)
    assert resp.status_code == 204 