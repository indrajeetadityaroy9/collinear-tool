from fastapi import APIRouter, HTTPException
import httpx
from app.core.config import settings
from typing import Dict, Any
from app.services.redis_client import cache_get, cache_set, generate_cache_key, cache_invalidate
import asyncio
import uuid
import time
from datetime import datetime, timezone

router = APIRouter(tags=["health"])

async def supabase_alive(timeout: float = 2.0) -> bool:
    url = f"{settings.SUPABASE_URL}/auth/v1/health"
    headers = {
        "apikey": settings.SUPABASE_ANON_KEY.get_secret_value()
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": {}
    }
    
    # Check Redis if enabled
    if settings.ENABLE_REDIS_CACHE:
        try:
            # Generate a random key to test Redis
            test_key = generate_cache_key("health", "check")
            test_value = {"timestamp": time.time(), "id": str(uuid.uuid4())}
            
            # Test write
            await cache_set(test_key, test_value, expire=60)
            
            # Test read
            cached_value = await cache_get(test_key)
            
            # Test delete
            await cache_invalidate(test_key)
            
            redis_ok = cached_value is not None and cached_value.get("timestamp") == test_value.get("timestamp")
            
            health_data["services"]["redis"] = {
                "status": "ok" if redis_ok else "degraded",
                "message": "Cache operations successful" if redis_ok else "Cache read/write inconsistency detected"
            }
        except Exception as e:
            health_data["services"]["redis"] = {
                "status": "error",
                "message": f"Redis error: {str(e)}"
            }
            health_data["status"] = "degraded"
    
    # Check Celery status if available
    try:
        from app.tasks.dataset_tasks import DatasetImpactTask
        stats = DatasetImpactTask.get_stats()
        
        health_data["services"]["celery"] = {
            "status": "ok",
            "tasks_processed": stats["processed"],
            "tasks_errors": stats["errors"],
            "cache_hits": stats["cache_hits"]
        }
    except ImportError:
        # Celery not available, which is OK
        pass
    except Exception as e:
        health_data["services"]["celery"] = {
            "status": "error",
            "message": f"Celery error: {str(e)}"
        }
        health_data["status"] = "degraded"
    
    # If any services are degraded, mark overall health as degraded
    for service_name, service_info in health_data["services"].items():
        if service_info.get("status") not in ["ok", "available"]:
            health_data["status"] = "degraded"
            break
    
    return health_data

@router.get("/live")
async def liveness_check():
    """Simple endpoint to check if the API is alive."""
    return {"status": "alive"}

@router.get("/ready")
async def readiness_check():
    """
    Readiness probe that checks if all required services are available.
    Used by Kubernetes and other orchestrators to determine if traffic should be sent to this instance.
    """
    checks = {"status": "ready", "checks": {}}
    
    # Check Redis connectivity
    if settings.ENABLE_REDIS_CACHE:
        try:
            test_key = generate_cache_key("health", "ready")
            await cache_set(test_key, {"timestamp": time.time()}, expire=60)
            checks["checks"]["redis"] = "ok"
        except Exception as e:
            checks["status"] = "not_ready"
            checks["checks"]["redis"] = f"error: {str(e)}"
    
    return checks

@router.get("/mock-test")
async def mock_test(delay: float = 0.1):
    """
    Test endpoint that simulates processing delay.
    Used for testing caching performance.
    """
    # Simulate processing time
    await asyncio.sleep(delay)
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "test_id": str(uuid.uuid4()),
        "delay": delay
    }

@router.get("/mock-dataset-impact")
async def mock_dataset_impact(dataset_id: str, skip_cache: bool = False) -> Dict[str, Any]:
    """
    Mock dataset impact endpoint for testing caching functionality.
    This endpoint intentionally has a delay to simulate work.
    """
    cache_key = generate_cache_key("mock-impact", dataset_id)
    
    # Try to get from cache if caching is enabled and not skipping cache
    if settings.ENABLE_REDIS_CACHE and not skip_cache:
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
    if settings.ENABLE_REDIS_CACHE and not skip_cache:
        await cache_set(cache_key, result, expire=60 * 5)  # 5 minutes
    
    return result
