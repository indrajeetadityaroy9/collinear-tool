from __future__ import annotations

import os
from typing import Final, Optional

from pydantic import SecretStr, HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Core application settings.
    Reads environment variables and .env file.
    """
    # Supabase Settings
    SUPABASE_URL: HttpUrl
    SUPABASE_SERVICE_KEY: SecretStr
    SUPABASE_ANON_KEY: SecretStr
    SUPABASE_JWT_SECRET: Optional[SecretStr] = None # Optional for local dev

    # Hugging Face API Token
    HF_API_TOKEN: Optional[SecretStr] = None

    # Redis settings
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_PASSWORD: Optional[SecretStr] = None
    
    # Caching settings
    ENABLE_REDIS_CACHE: bool = True
    CACHE_TTL: int = 3600  # Default: 1 hour

    # Worker settings
    WORKER_CONCURRENCY: int = 5
    WORKER_BATCH_SIZE: int = 50

    # Tell pydantic-settings to auto-load `.env` if present
    model_config: Final = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )

# Single, shared instance of settings
settings = Settings()
