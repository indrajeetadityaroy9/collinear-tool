#!/usr/bin/env bash
set -euo pipefail

USE_DOCKER=${USE_DOCKER:-true}
export BASE_URL="${BASE_URL:-http://localhost:8000/api}"
export EMAIL="${EMAIL:-testuser@example.com}"
export PASSWORD="${PASSWORD:-testpassword123}"

echo "Running API tests with pytest..."

if [ "$USE_DOCKER" = true ]; then
  echo "Using Docker container for tests..."
  docker-compose exec -T api pytest tests/
else
  echo "Running tests locally..."
  pytest tests/
fi

