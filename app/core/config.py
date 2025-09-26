import os
from typing import Final, Optional
from pydantic import SecretStr, HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    SUPABASE_URL: Optional[HttpUrl] = None
    SUPABASE_SERVICE_KEY: Optional[SecretStr] = None
    SUPABASE_ANON_KEY: Optional[SecretStr] = None
    SUPABASE_JWT_SECRET: Optional[SecretStr] = None

    HF_API_TOKEN: Optional[SecretStr] = None

    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_PASSWORD: Optional[SecretStr] = None

    ENABLE_REDIS_CACHE: bool = True

    SECRET_KEY: SecretStr = Field("change-me", env="SECRET_KEY")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(
        60 * 24 * 7, env="ACCESS_TOKEN_EXPIRE_MINUTES"
    )

    WORKER_CONCURRENCY: int = 10

    DATASET_BATCH_CHUNK_SIZE: int = 50

    model_config: Final = SettingsConfigDict(
        env_file=".env", case_sensitive=False, extra="ignore"
    )

    def resolve_hf_token(self, explicit_token: Optional[str] = None) -> Optional[str]:
        """Return a usable Hugging Face token from explicit value, settings, or env."""

        if explicit_token:
            return explicit_token
        if self.HF_API_TOKEN:
            return self.HF_API_TOKEN.get_secret_value()
        return os.environ.get("HUGGINGFACEHUB_API_TOKEN")


settings = Settings()
