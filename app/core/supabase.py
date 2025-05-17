from functools import lru_cache
from supabase import create_client, Client
from app.core.config import settings

@lru_cache
def get_client() -> Client:
    return create_client(settings.supabase_url, settings.supabase_anon_key)
