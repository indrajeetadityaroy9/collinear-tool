import asyncio
from app.core.supabase import get_client

client = get_client()

class UserRepo:
    async def register(self, email: str, password: str):
        return await asyncio.to_thread(
            lambda: client.auth.sign_up({"email": email, "password": password})
        )

    async def login(self, email: str, password: str):
        return await asyncio.to_thread(
            lambda: client.auth.sign_in_with_password({"email": email, "password": password})
        )
