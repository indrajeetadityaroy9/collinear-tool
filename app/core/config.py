import os

class Settings:
    def __init__(self):
        self.SUPABASE_URL = os.getenv('SUPABASE_URL')
        self.SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY')
        self.SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
        self.SUPABASE_JWT_SECRET = os.getenv('SUPABASE_JWT_SECRET')
        self.HF_API_TOKEN = os.getenv('HF_API_TOKEN')
        self.REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        self.REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
        self.ENABLE_REDIS_CACHE = os.getenv('ENABLE_REDIS_CACHE', 'True').lower() == 'true'
        self.SECRET_KEY = os.getenv('SECRET_KEY', 'change-me')
        self.ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', str(60 * 24 * 7)))
        self.WORKER_CONCURRENCY = int(os.getenv('WORKER_CONCURRENCY', '10'))
        self.DATASET_BATCH_CHUNK_SIZE = int(os.getenv('DATASET_BATCH_CHUNK_SIZE', '50'))

    def resolve_hf_token(self, explicit_token=None):
        if explicit_token:
            return explicit_token
        if self.HF_API_TOKEN:
            return self.HF_API_TOKEN
        return os.getenv('HUGGINGFACEHUB_API_TOKEN')

settings = Settings()
