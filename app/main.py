from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.api import api_router
from app.api.health import supabase_alive
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
    force=True,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("collinear")

@asynccontextmanager
async def lifespan(app: FastAPI):
    if await supabase_alive():
        logging.info("Supabase connectivity check passed")
    else:
        logging.warning("Supabase unreachable")
    yield

app = FastAPI(title="Collinear API", lifespan=lifespan)
app.include_router(api_router, prefix="/api")
