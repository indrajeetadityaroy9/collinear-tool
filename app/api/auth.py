from fastapi import APIRouter, Depends, HTTPException, status
from datetime import datetime, timezone
from app.schemas.auth import RegisterIn, LoginIn, SessionOut
from app.services.auth_service import AuthService
from app.api.dependencies import get_auth_service

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/register", response_model=SessionOut, status_code=status.HTTP_201_CREATED,
            responses={status.HTTP_202_ACCEPTED: {"description": "Signup succeeded — check your e-mail to confirm."}})
async def register(body: RegisterIn, auth_service: AuthService = Depends(get_auth_service)):
    session = await auth_service.register(body)
    if session is None:
        # This matches the case where AuthService.register returns None for email confirmation pending
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail="Signup succeeded — check your e-mail to confirm.",
        )
    return session # This is already a SessionOut object

@router.post("/login", response_model=SessionOut)
async def login(body: LoginIn, auth_service: AuthService = Depends(get_auth_service)):
    session = await auth_service.login(body)
    return session # This is already a SessionOut object
