# app/supabase.py
from __future__ import annotations

import os
from typing import Final, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
from supabase import Client, create_client


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
    model_config: Final = SettingsConfigDict(env_file=".env", case_sensitive=False)


def _build_client() -> Optional[Client]:
    """
    Return a live Supabase client **or** None when credentials are absent.

    That means:
    • local dev → put the two vars in .env; client is created
    • CI / unit-tests that don’t need Supabase → env vars missing; client is None
    """
    try:
        s = _Settings()                     # raises ValidationError if missing
    except Exception as exc:                # noqa: BLE001 – want *any* pydantic error
        # You can replace this with logging if preferred
        missing = {"SUPABASE_URL", "SUPABASE_SERVICE_KEY"} - os.environ.keys()
        print(f"[supabase] skipped – missing {', '.join(sorted(missing))} ({exc})")
        return None

    return create_client(s.SUPABASE_URL, s.SUPABASE_SERVICE_KEY)


#: single, shared instance – may be ``None`` in test/CI environments
supabase: Optional[Client] = _build_client()


# ──────────────────────────────────────────────────────────────────────
# Helper for service-layer code
# ──────────────────────────────────────────────────────────────────────
def require_supabase() -> Client:
    """
    Ensure a live client is available.

    Call this **inside** any endpoint/service that really needs Supabase:

        db = require_supabase()
        db.table("dataset_follows").insert({...}).execute()

    If the client is not configured the function raises a RuntimeError (→ FastAPI
    will translate that into HTTP 500 unless you trap it yourself).
    """
    if supabase is None:
        raise RuntimeError(
            "Supabase credentials are not configured. "
            "Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables."
        )
    return supabase
