from fastapi import APIRouter
from . import auth, users, datasets, follows, health
from app.api.datasets import router as datasets_router
from app.api.follows import router as follows_router
from app.api.auth import router as auth_router

api_router = APIRouter()

api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(datasets_router, tags=["datasets"])
api_router.include_router(follows_router, tags=["follows"])
api_router.include_router(auth_router, tags=["auth"])
api_router.include_router(users.router, tags=["users"])
