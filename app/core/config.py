import os
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    supabase_url = Field(default=None)
    supabase_service_key = Field(default=None)
    supabase_anon_key = Field(default=None)
    supabase_jwt_secret = Field(default=None)
    hf_api_token = Field(default=None)
    redis_url = Field(default='redis://localhost:6379/0')
    redis_password = Field(default=None)
    enable_redis_cache = Field(default=True)
    secret_key = Field(default='change-me-in-production')
    access_token_expire_minutes = Field(default=60 * 24 * 7)
    cors_origins = Field(
        default=['http://localhost:5173']
    )
    worker_concurrency = Field(default=10)
    dataset_batch_chunk_size = Field(default=50)
    api_host = Field(default='0.0.0.0')
    api_port = Field(default=8000)
    api_reload = Field(default=False)
    log_level = Field(default='INFO')

    @field_validator('cors_origins', mode='before')
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(',')]
        return v

    @field_validator('redis_url')
    @classmethod
    def validate_redis_url(cls, v):
        if not v.startswith(('redis://', 'rediss://')):
            raise ValueError('Redis URL must start with redis:// or rediss://')
        return v

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'Log level must be one of {valid_levels}')
        return v.upper()

    @field_validator('worker_concurrency')
    @classmethod
    def validate_worker_concurrency(cls, v):
        if v < 1 or v > 100:
            raise ValueError('Worker concurrency must be between 1 and 100')
        return v

    def resolve_hf_token(self, explicit_token=None):
        if explicit_token:
            return explicit_token
        if self.hf_api_token:
            return self.hf_api_token
        return os.getenv('HUGGINGFACEHUB_API_TOKEN')

    model_config = {
        'env_file': '.env',
        'case_sensitive': False,
        'json_schema_extra': {
            'fields': {
                'cors_origins': {
                    'description': 'Comma-separated list of allowed CORS origins'
                },
                'redis_url': {
                    'description': 'Redis connection URL'
                },
                'worker_concurrency': {
                    'description': 'Number of concurrent Celery workers'
                }
            }
        }
    }


Settings.WORKER_CONCURRENCY = property(lambda self: self.worker_concurrency)
Settings.REDIS_URL = property(lambda self: self.redis_url)
Settings.CORS_ORIGINS = property(lambda self: self.cors_origins)
Settings.HF_API_TOKEN = property(lambda self: self.hf_api_token)
Settings.ENABLE_REDIS_CACHE = property(lambda self: self.enable_redis_cache)

settings = Settings()