import asyncio
import httpx
from app.core.supabase import get_cached_supabase_anon_client
from app.schemas.user import UserCreate, UserUpdate, User
from app.core.config import settings
from fastapi import HTTPException

class UserRepo:
    def __init__(self):
        self.db = get_cached_supabase_anon_client()
        self.table_name = "user_profiles"

    async def get_user_by_id(self, user_id: str) -> User | None:
        try:
            response = await self.db.table(self.table_name).select("*").eq("id", user_id).maybe_single().execute()
            if response.data:
                return User(**response.data)
            return None
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=e.response.json())
        except Exception as e:
            print(f"Error in get_user_by_id: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def get_user_by_email(self, email: str) -> User | None:
        try:
            response = await self.db.table(self.table_name).select("*").eq("email", email).maybe_single().execute()
            if response.data:
                return User(**response.data)
            return None
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=e.response.json())
        except Exception as e:
            print(f"Error in get_user_by_email: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def create_user(self, user_create: UserCreate) -> User | None:
        try:
            user_data = user_create.model_dump()
            if 'id' not in user_data or not user_data['id']:
                raise HTTPException(status_code=400, detail="User ID (from auth user) must be provided to create profile")
            
            response = await self.db.table(self.table_name).insert(user_data).execute()
            if response.data:
                return User(**response.data[0])
            return None
        except httpx.HTTPStatusError as e:
            error_detail = "User profile creation conflict or error."
            try: supa_error = e.response.json(); error_detail = supa_error.get("message", error_detail) 
            except: pass
            raise HTTPException(status_code=e.response.status_code, detail=error_detail)
        except Exception as e:
            print(f"Error in create_user: {e}")
            raise HTTPException(status_code=500, detail="Internal server error creating user profile")

    async def update_user(self, user_id: str, user_update: UserUpdate) -> User | None:
        try:
            update_data = user_update.model_dump(exclude_unset=True)
            if not update_data:
                return await self.get_user_by_id(user_id)

            response = await self.db.table(self.table_name).update(update_data).eq("id", user_id).execute()
            
            if response.data:
                return User(**response.data[0])
            else:
                existing_user = await self.get_user_by_id(user_id)
                if not existing_user:
                    raise HTTPException(status_code=404, detail="User not found")
                return existing_user
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=e.response.json())
        except Exception as e:
            print(f"Error in update_user: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def delete_user(self, user_id: str) -> bool:
        try:
            response = await self.db.table(self.table_name).delete().eq("id", user_id).execute()
            return bool(response.data and len(response.data) > 0)
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=e.response.json())
        except Exception as e:
            print(f"Error in delete_user: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

# Example of how you might get the Supabase client for other operations if needed
# This is just illustrative and depends on your auth flow
async def get_authenticated_user_client(token: str):
    # This function would typically involve validating the token
    # and then creating a Supabase client instance with that token.
    # For simplicity, this is not fully implemented here.
    # You'd use settings.SUPABASE_URL and the token.
    # from supabase import create_client, Client
    # return create_client(settings.SUPABASE_URL, token) # This is a synchronous example
    # For async, you would use postgrest_async with an async httpx client
    pass
