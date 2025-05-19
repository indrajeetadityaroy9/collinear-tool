FROM python:3.11-slim

# 1. basic tooling you'll actually compile against
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential redis-tools && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. install deps first for better cache
COPY compose/local/fastapi/entrypoint /entrypoint
COPY compose/local/fastapi/api/start /start-api
COPY compose/local/fastapi/celery/worker/start /start-celeryworker
COPY compose/local/fastapi/celery/beat/start /start-celerybeat

# make them executable
RUN chmod +x /entrypoint /start-api /start-celeryworker /start-celerybeat

# install python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. copy project
COPY . .

# 4. run whichever entrypoint you bind in docker-compose
ENTRYPOINT ["/entrypoint"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]