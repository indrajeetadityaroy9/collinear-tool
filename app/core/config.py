from __future__ import annotations

from typing import Final, Optional

from pydantic import SecretStr, HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Core application settings.
    Reads environment variables and .env file.
    """

    SUPABASE_URL: HttpUrl
    SUPABASE_SERVICE_KEY: SecretStr
    SUPABASE_ANON_KEY: SecretStr
    SUPABASE_JWT_SECRET: Optional[SecretStr] = None


    HF_API_TOKEN: Optional[SecretStr] = None


    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_PASSWORD: Optional[SecretStr] = None


    ENABLE_REDIS_CACHE: bool = True



    SECRET_KEY: SecretStr = Field("change-me", env="SECRET_KEY")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(60 * 24 * 7, env="ACCESS_TOKEN_EXPIRE_MINUTES")


    WORKER_CONCURRENCY: int = 10


    DATASET_BATCH_CHUNK_SIZE: int = 50


    model_config: Final = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )


settings = Settings()
