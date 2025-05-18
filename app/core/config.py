from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import HttpUrl, SecretStr, Field


class Settings(BaseSettings):
    supabase_url: HttpUrl = Field(..., env="SUPABASE_URL")
    supabase_anon_key: SecretStr = Field(..., env="SUPABASE_ANON_KEY")
    supabase_service_key: SecretStr | None = Field(None, env="SUPABASE_SERVICE_KEY")
    hf_api_token: SecretStr | None = Field(None, env="HF_API_TOKEN")

    model_config = SettingsConfigDict(
        env_file=".env",      
        env_prefix="",         
        extra="ignore",  # Allow extra variables in .env
    )

settings = Settings()
