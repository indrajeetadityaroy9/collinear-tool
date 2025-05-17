from fastapi import Header, HTTPException, status
import httpx
from app.core.config import settings
from app.models.user import User

async def current_user(
    authorization: str = Header(..., alias="Authorization"),
) -> User:
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid auth header")

    token = authorization.split(" ", 1)[1]

    url = f"{settings.supabase_url}/auth/v1/user"
    headers = {
        "apikey": settings.supabase_anon_key,
        "Authorization": f"Bearer {token}",
    }

    async with httpx.AsyncClient(timeout=3) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code != 200:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token")

    data = resp.json()
    return User(id=data["id"], email=data.get("email"))
