from fastapi import APIRouter
from . import auth, users, health

api_router = APIRouter()
api_router.include_router(auth.router)
api_router.include_router(users.router)
api_router.include_router(health.router)
