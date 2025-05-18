#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="uvicorncollinearapi"
RUNTIME_PKGS=(
  "fastapi[all]>=0.111"
  "supabase>=2.0"
  pydantic-settings
  python-dotenv
  httpx
  anyio
  huggingface_hub
)
DEV_PKGS=(ruff mypy pytest pytest-asyncio pre-commit)

python3 -m pip install --upgrade pipx >/dev/null
pipx ensurepath

echo "(Re)installing env '$ENV_NAME' …"
pipx install --include-deps --force "uvicorn[standard]" --suffix collinearapi

echo "Injecting runtime pkgs …"
pipx inject "$ENV_NAME" "${RUNTIME_PKGS[@]}"

echo "Injecting dev tools …"
pipx inject "$ENV_NAME" "${DEV_PKGS[@]}"

BIN_PATH=$(command -v "$ENV_NAME" || true)
if [[ -n "$BIN_PATH" ]]; then
  ln -sf "$BIN_PATH" "$(dirname "$BIN_PATH")/collinearapi"
  echo "🔗  Alias created: $(dirname "$BIN_PATH")/collinearapi → $ENV_NAME"
else
  echo "❗  Could not find executable $ENV_NAME on PATH — alias skipped." >&2
fi

echo -e "\nReady!"
echo "   Start dev server :  collinearapi app.main:app --reload"
echo "   Lint / format    :  ruff check app/ && ruff format app/"
echo "   Type-check       :  mypy app/"
echo "   Run tests        :  pytest -q"
