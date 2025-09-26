import os
from typing import List, Optional, Union
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    hf_api_token: Optional[str] = Field(default=None)
    redis_url: str = Field(default='redis://localhost:6379/0')
    cors_origins: Union[List[str], str] = Field(
        default=['http://localhost:5173']
    )
    worker_concurrency: int = Field(default=10)

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
settings = Settings()