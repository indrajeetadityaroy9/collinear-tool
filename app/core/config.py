from __future__ import annotations

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
    
    # Toggle Redis cache layer
    ENABLE_REDIS_CACHE: bool = True

    # ──────────────────────────────── Security ────────────────────────────────
    # JWT secret key. NEVER hard-code in source; override with env variable in production.
    SECRET_KEY: SecretStr = Field("change-me", env="SECRET_KEY")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(60 * 24 * 7, env="ACCESS_TOKEN_EXPIRE_MINUTES")  # 1 week by default

    # Worker settings
    WORKER_CONCURRENCY: int = 10  # Increased from 5 for better parallel performance

    # Batch processing chunk size for Celery dataset tasks
    DATASET_BATCH_CHUNK_SIZE: int = 50

    # Tell pydantic-settings to auto-load `.env` if present
    model_config: Final = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )

# Single, shared instance of settings
settings = Settings()
