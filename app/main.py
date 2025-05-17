from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import logging, sys

from app.core.health import supabase_alive

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
    force=True,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("collinear")

@asynccontextmanager
async def lifespan(app: FastAPI):
    if await supabase_alive():
        logger.info("✅ Supabase connectivity check passed")
    else:
        logger.warning("⚠️ Supabase unreachable")
    yield

app = FastAPI(title="Collinear API", lifespan=lifespan)

@app.get("/health/supabase", include_in_schema=False)
async def health_supabase():
    if not await supabase_alive():
        raise HTTPException(503, "Supabase unreachable")
    return {"status": "ok"}

