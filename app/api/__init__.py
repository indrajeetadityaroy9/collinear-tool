from fastapi import APIRouter
from app.api.datasets import router as datasets_router

api_router = APIRouter()
api_router.include_router(datasets_router, tags=["datasets"]) 