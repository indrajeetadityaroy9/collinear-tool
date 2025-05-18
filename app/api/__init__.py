from fastapi import APIRouter
from . import auth, users, datasets, follows, health

api_router = APIRouter()

api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(datasets.router, tags=["datasets"])
api_router.include_router(follows.router, tags=["follows"])
api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(users.router, tags=["users"])
