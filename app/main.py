import logging
import json
import asyncio
from fastapi import FastAPI
from app.api import api_router
from app.core.config import settings
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {'level': record.levelname, 'time': self.formatTime(record, self.datefmt), 'name': record.name, 'message': record.getMessage()}
        if record.exc_info:
            log_record['exc_info'] = self.formatException(record.exc_info)
        return json.dumps(log_record)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)
app = FastAPI(title='Collinear API')
from app.middleware.memory_profiler import MemoryProfilerMiddleware
app.add_middleware(
    MemoryProfilerMiddleware,
    memory_limit_mb=1000,
    gc_threshold_mb=500,
    log_requests=False
)
app.add_middleware(CORSMiddleware, allow_origins=settings.CORS_ORIGINS, allow_credentials=True, allow_methods=['*'], allow_headers=['*'])
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(api_router, prefix='/api')


@app.on_event("startup")
async def startup_event():
    logger.info("Starting application...")
    try:
        from app.integrations.redis_client import get_redis, cache_get
        from app.integrations.redis_search import create_sort_index
        redis_client = await get_redis()
        meta = await cache_get('hf:datasets:meta')
        if not meta or meta.get('total_items', 0) == 0:
            logger.info("Cache is empty, triggering initial cache population...")
            from app.tasks.dataset_tasks import refresh_hf_datasets_full_cache
            task = refresh_hf_datasets_full_cache.delay()
            logger.info(f"Cache population task started: {task.id}")
        else:
            logger.info(f"Cache already populated with {meta.get('total_items', 0)} items")
            logger.info("Creating sort indexes for efficient sorting...")
            for field in ['downloads', 'likes', 'size_bytes']:
                try:
                    import asyncio
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, create_sort_index, field)
                    logger.info(f"Created sort index for field: {field}")
                except Exception as exc:
                    logger.warning(f"Failed to create sort index for {field}: {exc}")
    except Exception as exc:
        logger.error(f"Error during cache warming: {exc}")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down application...")
    try:
        from app.utils.http_client import close_hf_client
        await close_hf_client()
        logger.info("Closed HTTP clients")
    except Exception as exc:
        logger.error(f"Error closing HTTP clients: {exc}")


@app.get('/')
async def root():
    return {'message': 'Welcome to the Collinear Data Tool API'}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('app.main:app', host='0.0.0.0', port=8000, reload=True)