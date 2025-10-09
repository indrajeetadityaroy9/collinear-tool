FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential redis-tools && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY compose/local/fastapi/entrypoint /entrypoint
COPY compose/local/fastapi/api/start /start-api
COPY compose/local/fastapi/celery/worker/start /start-celeryworker
COPY compose/local/fastapi/celery/beat/start /start-celerybeat

RUN chmod +x /entrypoint /start-api /start-celeryworker /start-celerybeat

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["bash", "/entrypoint"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
