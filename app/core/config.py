from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import HttpUrl

class Settings(BaseSettings):
    supabase_url:  HttpUrl
    supabase_anon_key: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
