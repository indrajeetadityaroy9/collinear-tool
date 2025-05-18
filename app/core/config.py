from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import HttpUrl, SecretStr

class Settings(BaseSettings):
    supabase_url: HttpUrl
    supabase_anon_key: SecretStr
    supabase_service_key: SecretStr | None = None
    hf_api_token: SecretStr | None = None

    model_config = SettingsConfigDict(
        env_file=".env",      
        env_prefix="",         
        extra="ignore",
    )

settings = Settings()
