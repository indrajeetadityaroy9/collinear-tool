from fastapi import APIRouter, HTTPException
import httpx
from app.core.config import settings

router = APIRouter(prefix="/health", tags=["health"])

async def supabase_alive(timeout: float = 2.0) -> bool:
    url = f"{settings.supabase_url}/auth/v1/health"
    headers = {"apikey": settings.supabase_anon_key}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url, headers=headers)
            return 200 <= r.status_code < 300
    except httpx.HTTPError:
        return False

@router.get("/supabase", include_in_schema=False)
async def health_supabase():
    if not await supabase_alive():
        raise HTTPException(503, "Supabase unreachable")
    return {"status": "ok"}

@router.get("/live", include_in_schema=False)
async def health_live():
    return {"status": "alive"}
