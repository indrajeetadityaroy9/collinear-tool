from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from gotrue.errors import AuthApiError

from app.api.dependencies import current_user, User
from app.schemas.auth import SessionOut, RefreshIn, UpdateUserIn
from app.core.config import settings
from app.core.supabase import get_client
from app.services.follows import list_followed_datasets

router = APIRouter(prefix="/users", tags=["users"])
client = get_client()                  

def _session_to_out(s) -> SessionOut:
    return SessionOut(
        access_token=s.access_token,
        expires_at=datetime.fromtimestamp(s.expires_at, tz=timezone.utc),
    )

@router.get("/me", summary="Return the authenticated user")
async def read_current_user(user: User = Depends(current_user)):
    return {"id": user.id, "email": user.email}


@router.patch(
    "/me",
    summary="Update e-mail and/or password",
    status_code=status.HTTP_202_ACCEPTED,
)
async def update_me(
    payload: UpdateUserIn,
    authorization: Annotated[str, Header(alias="Authorization")] = ...,
):
    body: dict = {}
    if payload.email:
        body["email"] = payload.email
    if payload.password:
        body["password"] = payload.password.get_secret_value()
    if not body:
        raise HTTPException(400, "Nothing to update")

    token = authorization.split(" ", 1)[1]
    url = f"{settings.supabase_url}/auth/v1/user"
    headers = {
        "apikey": settings.supabase_anon_key,
        "Authorization": f"Bearer {token}",
    }

    async with httpx.AsyncClient(timeout=3) as http:
        r = await http.put(url, headers=headers, json=body)

    if r.status_code != 200:
        raise HTTPException(400, r.json().get("msg", "Update failed"))

    return {"detail": "Profile updated — please log in again."}

@router.post(
    "/refresh",
    summary="Exchange refresh token for a new access token",
    response_model=SessionOut,
)
async def refresh_token(body: RefreshIn):
    try:
        res = client.auth.refresh_session(body.refresh_token)
    except AuthApiError as e:
        raise HTTPException(401, e.message)
    return _session_to_out(res.session)

@router.post(
    "/logout",
    summary="Invalidate current session",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def logout(
    authorization: Annotated[str, Header(alias="Authorization")] = ...,
):
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Invalid auth header")

    access_token = authorization.split(" ", 1)[1]

    url = f"{settings.supabase_url}/auth/v1/logout"
    headers = {
        "apikey": settings.supabase_anon_key,
        "Authorization": f"Bearer {access_token}",
    }

    async with httpx.AsyncClient(timeout=3) as http:
        await http.post(url, headers=headers)

@router.get("/me/follows")
async def my_follows_endpoint(
    limit: int | None = Query(None, ge=1),
    offset: int = 0,
    user: User = Depends(current_user),
    session: AsyncSession = Depends(get_db),
):
    ids = await list_followed_datasets(user.id, limit=limit, offset=offset)
    return ids
