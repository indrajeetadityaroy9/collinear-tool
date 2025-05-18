# app/supabase.py
from __future__ import annotations

from typing import Optional

from supabase import Client, create_client
from app.core.config import settings # Import the unified settings

# This module primarily provides a way to get a new Supabase client.
# The global/cached client pattern will be handled by dependency injectors if needed.

def get_new_supabase_client(jwt: str | None = None, settings_override=None) -> Client:
    """
    Creates a new Supabase client.
    If jwt is provided, it attempts to set the session for the client.
    Raises RuntimeError if essential Supabase settings are not configured.
    
    Args:
        jwt: Optional JWT token for authentication
        settings_override: Optional settings object to use instead of the global settings
    """
    # Use provided settings or fall back to global settings
    config = settings_override or settings
    
    if not config.SUPABASE_URL or not config.SUPABASE_SERVICE_KEY:
        raise RuntimeError(
            "Supabase core settings (URL and SERVICE_KEY) are not configured. "
            "Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables."
        )
    
    # Convert HttpUrl to string to avoid TypeError in Supabase client
    supabase_url = str(config.SUPABASE_URL)
    
    # Use SUPABASE_SERVICE_KEY for general client creation by default.
    # For user-specific actions, the JWT will authorize at the row-level security.
    client = create_client(supabase_url, config.SUPABASE_SERVICE_KEY.get_secret_value())
    
    if jwt:
        # For authenticate user actions requiring refresh token
        if jwt != "dummy_refresh_token" and "." in jwt:  # Simple check for JWT format
            # Note: The Supabase Python client's set_session is synchronous.
            # If called in async code, consider `asyncio.to_thread` if it becomes a bottleneck.
            client.auth.set_session(access_token=jwt, refresh_token="dummy_refresh_token")
    return client

# The old global `supabase: Optional[Client]` and `require_supabase` are being phased out
# in favor of dependency injection. If some non-request-scoped code still needs a client,
# it can call get_new_supabase_client() or we can introduce a cached client via settings/DI later.
