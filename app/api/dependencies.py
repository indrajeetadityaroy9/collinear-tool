from fastapi import Header, HTTPException, status
from jose import jwt, JWTError
from app.schemas.user import User
import os

SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "")  # Set this in your .env for production

async def current_user(
    authorization: str = Header(..., alias="Authorization"),
) -> User:
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid auth header")

    token = authorization.split(" ", 1)[1]

    try:
        if SUPABASE_JWT_SECRET:
            payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"])
        else:
            # For local dev, skip verification (NOT for production)
            payload = jwt.get_unverified_claims(token)
        user_id = payload.get("sub")
        email = payload.get("email")
        if not user_id:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token: no user id")
        return User(id=user_id, email=email)
    except JWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token")
