# app/supabase.py
from __future__ import annotations

import os
from typing import Final, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
from supabase import Client, create_client


print(os.environ.get("SUPABASE_URL"))
print(os.environ.get("SUPABASE_ANON_KEY"))

class _Settings(BaseSettings):
    """
    Reads SUPABASE_URL and SUPABASE_SERVICE_KEY.

    Order of precedence (highest → lowest):
      1. explicit `env` kwargs when _Settings() is instantiated
      2. process environment – os.environ
      3. entries in the .env file (repo root by default)
    """
    SUPABASE_URL: str
    SUPABASE_SERVICE_KEY: str

    # Tell pydantic-settings to auto-load `.env` if present
    model_config: Final = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")


def _build_client() -> Optional[Client]:
    """
    Return a live Supabase client **or** None when credentials are absent.

    That means:
    • local dev → put the two vars in .env; client is created
    • CI / unit-tests that don't need Supabase → env vars missing; client is None
    """
    try:
        s = _Settings()                     # raises ValidationError if missing
    except Exception as exc:                # noqa: BLE001 – want *any* pydantic error
        # You can replace this with logging if preferred
        missing = {"SUPABASE_URL", "SUPABASE_SERVICE_KEY"} - os.environ.keys()
        print(f"[supabase] skipped – missing {', '.join(sorted(missing))} ({exc})")
        return None

    return create_client(s.SUPABASE_URL, s.SUPABASE_SERVICE_KEY)


# Store these at module level
_settings = None
try:
    _settings = _Settings()
except Exception:
    pass

SUPABASE_URL = _settings.SUPABASE_URL if _settings else None
SUPABASE_SERVICE_KEY = _settings.SUPABASE_SERVICE_KEY if _settings else None

#: single, shared instance – may be ``None`` in test/CI environments
supabase: Optional[Client] = _build_client()


# ──────────────────────────────────────────────────────────────────────
# Helper for service-layer code
# ──────────────────────────────────────────────────────────────────────
def require_supabase(jwt: str | None = None) -> Client:
    """
    Ensure a live client is available. If jwt is provided, set the session for the client.
    """
    if SUPABASE_URL is None or SUPABASE_SERVICE_KEY is None:
        raise RuntimeError(
            "Supabase credentials are not configured. "
            "Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables."
        )
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    if jwt:
        client.auth.set_session(jwt, "")  # Pass empty string for refresh_token
    return client
