from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, status

from app.core.dependencies import get_current_user, get_user_service, get_auth_service, get_bearer_token
from app.schemas.user import User
from app.schemas.auth import SessionOut, RefreshIn, UpdateUserIn
from app.services.auth_service import AuthService, UserService
from app.services.follows import list_followed_datasets

router = APIRouter(prefix="/users", tags=["users"])

@router.get("/me", summary="Return the authenticated user", response_model=User)
async def read_current_user_endpoint(
    current_user_data: User = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
):
    return await user_service.get_me(current_user_data)

@router.patch(
    "/me",
    summary="Update e-mail and/or password",
    status_code=status.HTTP_202_ACCEPTED,
)
async def update_me_endpoint(
    payload: UpdateUserIn,
    token: str = Depends(get_bearer_token),
    auth_service: AuthService = Depends(get_auth_service),
):
    await auth_service.update_user_profile(token=token, payload=payload)
    return {"detail": "Profile update requested. If successful, you may need to log in again."}

@router.post(
    "/refresh",
    summary="Exchange refresh token for a new access token",
    response_model=SessionOut,
)
async def refresh_token_endpoint(
    body: RefreshIn,
    auth_service: AuthService = Depends(get_auth_service)
):
    return await auth_service.refresh_auth_session(body.refresh_token)

@router.post(
    "/logout",
    summary="Invalidate current session",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def logout_endpoint(
    token: str = Depends(get_bearer_token),
    auth_service: AuthService = Depends(get_auth_service),
):
    await auth_service.logout_user(token=token)
    return

@router.get("/me/follows")
async def my_follows_endpoint(
    user: User = Depends(get_current_user),
):
    return {"detail": "/me/follows endpoint needs refactoring with FollowsService"}
