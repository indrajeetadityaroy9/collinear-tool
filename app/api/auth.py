from fastapi import APIRouter, Depends, HTTPException, status
from gotrue.errors import AuthApiError
from datetime import datetime, timezone
from app.schemas.auth import RegisterIn, LoginIn, SessionOut
from app.repos.user_repo import UserRepo

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/register", response_model=SessionOut, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterIn, repo: UserRepo = Depends()):
    try:
        res = await repo.register(body.email, body.password.get_secret_value())
    except AuthApiError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, e.message)

    if res.session is None:
        return HTTPException(
            status.HTTP_202_ACCEPTED,
            detail="Signup succeeded — check your e-mail to confirm.",
        )

    s = res.session
    return SessionOut(
        access_token=s.access_token,
        expires_at=datetime.fromtimestamp(s.expires_at, tz=timezone.utc),
    )

@router.post("/login", response_model=SessionOut)
async def login(body: LoginIn, repo: UserRepo = Depends()):
    try:
        res = await repo.login(body.email, body.password.get_secret_value())
    except AuthApiError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, e.message)

    s = res.session
    return SessionOut(
        access_token=s.access_token,
        expires_at=datetime.fromtimestamp(s.expires_at, tz=timezone.utc),
    )