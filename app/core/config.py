from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import HttpUrl, SecretStr, Field


class Settings(BaseSettings):
    supabase_url: HttpUrl = Field(..., env="SUPABASE_URL")
    supabase_anon_key: SecretStr = Field(..., env="SUPABASE_ANON_KEY")
    supabase_service_key: SecretStr | None = Field(None, env="SUPABASE_SERVICE_KEY")
    hf_api_token: SecretStr | None = Field(None, env="HF_API_TOKEN")
    
    # Redis settings
    redis_url: str | None = Field("redis://localhost:6379/0", env="REDIS_URL")
    redis_password: SecretStr | None = Field(None, env="REDIS_PASSWORD")
    
    # Caching settings
    enable_redis_cache: bool = Field(True, env="ENABLE_REDIS_CACHE")
    cache_ttl: int = Field(3600, env="CACHE_TTL")  # Default: 1 hour
    
    # Worker settings
    worker_concurrency: int = Field(5, env="WORKER_CONCURRENCY")
    worker_batch_size: int = Field(50, env="WORKER_BATCH_SIZE")

    model_config = SettingsConfigDict(
        env_file=".env",      
        env_prefix="",         
        extra="ignore",  # Allow extra variables in .env
    )

settings = Settings()
