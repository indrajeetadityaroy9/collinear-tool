from fastapi import APIRouter
from app.api.routes import datasets
api_router = APIRouter()
api_router.include_router(datasets.router, tags=['datasets'])
