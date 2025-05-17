import httpx
from app.core.config import settings

async def supabase_alive(timeout: float = 2.0) -> bool:
    url = f"{settings.supabase_url}/auth/v1/health"
    headers = {
        "apikey": settings.supabase_anon_key,
        "Authorization": f"Bearer {settings.supabase_anon_key}",
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url, headers=headers)
            return 200 <= r.status_code < 300
    except httpx.HTTPError:
        return False

