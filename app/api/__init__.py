from fastapi import APIRouter
from app.api.datasets import router as datasets_router
# from . import batch # Removed batch import

api_router = APIRouter()
api_router.include_router(datasets_router, tags=["datasets"])
# api_router.include_router(batch.router) # Removed batch router 