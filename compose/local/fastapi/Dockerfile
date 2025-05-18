FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    # Use Google DNS servers directly in pip
    PIP_TIMEOUT=100

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libc6-dev \
    redis-tools \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create pip configuration with explicit DNS settings
RUN mkdir -p /root/.pip && \
    echo "[global]" > /root/.pip/pip.conf && \
    echo "timeout = 120" >> /root/.pip/pip.conf && \
    echo "index-url = https://pypi.org/simple" >> /root/.pip/pip.conf && \
    echo "trusted-host = pypi.org pypi.python.org files.pythonhosted.org" >> /root/.pip/pip.conf

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies - try with multiple pip mirrors
RUN pip install --no-cache-dir \
    -i https://pypi.org/simple/ \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org \
    -r requirements.txt || \
    pip install --no-cache-dir \
    -i https://pypi.tuna.tsinghua.edu.cn/simple/ \
    --trusted-host pypi.tuna.tsinghua.edu.cn \
    -r requirements.txt

# Copy scripts
COPY compose/local/fastapi/entrypoint /entrypoint
COPY compose/local/fastapi/api/start /start-api
COPY compose/local/fastapi/celery/worker/start /start-celeryworker
COPY compose/local/fastapi/celery/beat/start /start-celerybeat
COPY compose/local/fastapi/celery/flower/start /start-flower
COPY compose/local/fastapi/legacy-worker/start /start-legacy

# Make scripts executable
RUN chmod +x /entrypoint \
    /start-api \
    /start-celeryworker \
    /start-celerybeat \
    /start-flower \
    /start-legacy

# Copy project
COPY . .

# Create a non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
# Copy pip.conf to user home directory
RUN mkdir -p /home/appuser/.pip && \
    cp /root/.pip/pip.conf /home/appuser/.pip/ && \
    chown -R appuser:appuser /home/appuser/.pip

# Switch to non-root user
USER appuser

# Use the entrypoint script to run the specified command
ENTRYPOINT ["/entrypoint"]

# Default to running API service
CMD ["/start-api"] 