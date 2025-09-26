import logging
import json
import asyncio
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.api import api_router
from app.core.config import settings
from app.middleware.memory_profiler import MemoryProfilerMiddleware

from app.integrations.redis_client import get_redis, cache_get
from app.integrations.redis_search import create_sort_index
from app.tasks.dataset_tasks import refresh_hf_datasets_full_cache
from app.utils.http_client import close_hf_client


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


@asynccontextmanager
async def lifespan(app: FastAPI):

    # Startup
    logger.info("Starting application...")
    try:
        redis_client = await get_redis()
        meta = await cache_get('hf:datasets:meta')
        if not meta or meta.get('total_items', 0) == 0:
            logger.info("Cache is empty, triggering initial cache population...")
            task = refresh_hf_datasets_full_cache.delay()
            logger.info(f"Cache population task started: {task.id}")
        else:
            logger.info(f"Cache already populated with {meta.get('total_items', 0)} items")
            logger.info("Creating sort indexes for efficient sorting...")
            for field in ['downloads', 'likes', 'size_bytes']:
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, create_sort_index, field)
                    logger.info(f"Created sort index for field: {field}")
                except Exception as exc:
                    logger.warning(f"Failed to create sort index for {field}: {exc}")
    except Exception as exc:
        logger.error(f"Error during cache warming: {exc}")

    yield

    # Shutdown
    logger.info("Shutting down application...")
    try:
        await close_hf_client()
        logger.info("Closed HTTP clients")
    except Exception as exc:
        logger.error(f"Error closing HTTP clients: {exc}")


app = FastAPI(title='Collinear API', lifespan=lifespan)
app.add_middleware(
    MemoryProfilerMiddleware,
    memory_limit_mb=1000,
    gc_threshold_mb=500,
    log_requests=False
)
app.add_middleware(CORSMiddleware, allow_origins=settings.CORS_ORIGINS, allow_credentials=True, allow_methods=['*'], allow_headers=['*'])
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(api_router, prefix='/api')


@app.get('/')
async def root():
    return {'message': 'Welcome to the Collinear Data Tool API'}


if __name__ == '__main__':
    uvicorn.run('app.main:app', host='0.0.0.0', port=8000, reload=True)