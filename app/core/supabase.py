from functools import lru_cache
from supabase import create_client, Client
from app.core.config import settings

@lru_cache
def get_cached_supabase_anon_client() -> Client:
    """Returns a cached Supabase client initialized with the ANON key."""
    if not settings.SUPABASE_URL or not settings.SUPABASE_ANON_KEY:
        # Or handle this more gracefully depending on how critical this client is at startup
        raise RuntimeError("Supabase URL and ANON_KEY must be configured.")
    return create_client(
        str(settings.SUPABASE_URL),                   
        settings.SUPABASE_ANON_KEY.get_secret_value()
    )

@lru_cache
def get_cached_supabase_service_client() -> Client:
    """Returns a cached Supabase client initialized with the SERVICE_ROLE key."""
    if not settings.SUPABASE_URL or not settings.SUPABASE_SERVICE_KEY:
        raise RuntimeError("Supabase URL and SERVICE_KEY must be configured.")
    return create_client(
        str(settings.SUPABASE_URL),                   
        settings.SUPABASE_SERVICE_KEY.get_secret_value()
    )

# For clarity, the old get_client() is replaced by more specific versions.
# If app/api/users.py specifically needs the anon client for refresh:
# from app.core.supabase import get_cached_supabase_anon_client as get_client
