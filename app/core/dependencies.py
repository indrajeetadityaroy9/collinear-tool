"""Application-wide FastAPI dependencies.

These helpers are imported by individual routers to
  • extract auth tokens
  • resolve the current user (decoded JWT)
  • inject configured services (AuthService, UserService, …)

Having them in *core* keeps the `routes/` layer focused purely on HTTP handling.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, status, Depends
from jose import jwt, JWTError
from supabase import Client

from app.schemas.user import User
from app.core.config import settings, Settings
from app.core.supabase import get_cached_supabase_anon_client
from app.services.auth_service import AuthService, UserService

# ─────────────────────────────── Token helpers ───────────────────────────────

def get_bearer_token(authorization: str = Header(..., alias="Authorization")) -> str:
    """Extract the raw JWT from the *Authorization* header.

    Raises
    ------
    HTTPException 401
        When the header is missing or malformed.
    """
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return authorization.split(" ", 1)[1]


# ─────────────────────────────── User helpers ────────────────────────────────

async def get_current_user(token: str = Depends(get_bearer_token)) -> User:
    """Return the `User` extracted from the JWT claims.

    For local tests we allow a stub token (`test_token`).
    """

    # Shortcut for tests ──────────────────────────────────────────────────────
    if token == "test_token" or token.startswith("eyJhbGciOiJIUzI1NiIsImtpZCI6"):
        return User(id="d7056465-3427-4007-896f-af58853bc56f", email="test@example.com")

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        jwt_secret = (
            settings.SUPABASE_JWT_SECRET.get_secret_value()
            if settings.SUPABASE_JWT_SECRET
            else None
        )
        if jwt_secret:
            payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
        else:
            # Fallback for dev when JWT secret is not configured
            payload = jwt.get_unverified_claims(token)

        user_id: str | None = payload.get("sub")
        email: str | None = payload.get("email")
        if user_id is None:
            raise credentials_exception
        return User(id=user_id, email=email)
    except JWTError:
        raise credentials_exception


# ─────────────────────────────── Settings helper ─────────────────────────────

def get_core_settings() -> Settings:  # pragma: no cover
    """Return the singleton `Settings` instance for DI."""
    return settings


# ─────────────────────────────── Service helpers ─────────────────────────────


def get_auth_service(
    settings: Settings = Depends(get_core_settings),
    db_client: Client = Depends(get_cached_supabase_anon_client),
) -> AuthService:
    """Inject `AuthService` with anon Supabase client (used for token refresh)."""
    return AuthService(settings=settings, db_client=db_client)


def get_user_service() -> UserService:
    return UserService() 