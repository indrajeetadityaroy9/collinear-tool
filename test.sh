#!/usr/bin/env bash
set -euo pipefail

# Run all pytest tests in the tests/ directory

export BASE_URL="${BASE_URL:-http://127.0.0.1:8000/api}"
export EMAIL="${EMAIL:-testuser@example.com}"
export PASSWORD="${PASSWORD:-testpassword123}"

echo "Running API tests with pytest..."
pytest tests/

