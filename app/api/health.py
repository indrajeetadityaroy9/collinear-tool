from fastapi import APIRouter, HTTPException
import httpx
from app.core.config import settings
from typing import Dict, Any
from app.services.redis_client import cache_get, cache_set, generate_cache_key, cache_invalidate
import asyncio

router = APIRouter(prefix="/health", tags=["health"])

async def supabase_alive(timeout: float = 2.0) -> bool:
    url = f"{settings.supabase_url}/auth/v1/health"
    headers = {
        "apikey": settings.supabase_anon_key.get_secret_value()
    }
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

@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint to verify the API is running.
    
    Returns information about the API status and connected services.
    """
    # Basic health information
    health_data = {
        "status": "ok",
        "api": "running",
        "version": "1.0.0",
        "services": {}
    }
    
    # Check Redis if enabled
    if settings.enable_redis_cache:
        try:
            # Generate a random key to test Redis
            test_key = generate_cache_key("health", "check")
            await cache_set(test_key, "test", expire=10)
            test_result = await cache_get(test_key)
            await cache_invalidate(test_key)
            
            health_data["services"]["redis"] = {
                "status": "connected" if test_result == "test" else "error",
                "url": settings.redis_url.replace(":6379", ":*****"),  # Mask port for security
            }
        except Exception as e:
            health_data["services"]["redis"] = {
                "status": "error",
                "error": str(e)
            }
    
    return health_data

@router.get("/mock-test")
async def mock_test_endpoint() -> Dict[str, Any]:
    """
    Mock endpoint for testing caching functionality.
    This endpoint intentionally has a delay to simulate work.
    """
    # Introduce a delay to simulate processing work
    await asyncio.sleep(0.1)
    
    return {
        "status": "success",
        "message": "This response can be cached",
        "data": {
            "id": "mock-123",
            "name": "Test Dataset",
            "metrics": {
                "size_bytes": 1024 * 1024 * 100,  # 100 MB
                "file_count": 250,
                "downloads": 5000,
                "likes": 150
            }
        }
    }
 
@router.get("/mock-dataset-impact")
async def mock_dataset_impact(dataset_id: str, skip_cache: bool = False) -> Dict[str, Any]:
    """
    Mock dataset impact endpoint for testing caching functionality.
    This endpoint intentionally has a delay to simulate work.
    """
    cache_key = generate_cache_key("mock-impact", dataset_id)
    
    # Try to get from cache if caching is enabled and not skipping cache
    if settings.enable_redis_cache and not skip_cache:
        cached_data = await cache_get(cache_key)
        if cached_data:
            return cached_data
    
    # Introduce a delay to simulate processing work
    await asyncio.sleep(0.2)
    
    result = {
        "dataset_id": dataset_id,
        "impact_level": "medium",
        "assessment_method": "mock",
        "metrics": {
            "size_bytes": 1024 * 1024 * 100,  # 100 MB
            "file_count": 250,
            "downloads": 5000,
            "likes": 150
        },
        "thresholds": {
            "size": {
                "low": 1024 * 1024 * 10,  # 10 MB
                "medium": 1024 * 1024 * 500,  # 500 MB
                "high": 1024 * 1024 * 5000  # 5 GB
            },
            "popularity": {
                "low": 100,
                "medium": 1000,
                "high": 10000
            }
        }
    }
    
    # Cache the result
    if settings.enable_redis_cache and not skip_cache:
        await cache_set(cache_key, result, expire=60 * 5)  # 5 minutes
    
    return result
