#!/usr/bin/env bash
# ============================================================================
#  Bootstrap a pipx-managed FastAPI + Supabase dev environment
# ----------------------------------------------------------------------------
#  • Creates virtual-env: ~/.local/pipx/venvs/uvicorn-collinearapi
#  • Installs uvicorn[standard] with the suffix -collinearapi
#  • Injects runtime + dev dependencies into the same env
#  • Adds an easy-to-type wrapper:  ~/.local/bin/collinearapi
# ============================================================================

set -euo pipefail

ENV_NAME="uvicorncollinearapi"     # <- MUST match the venv name pipx creates
BIN_DIR="${HOME}/.local/bin"

# Runtime dependencies (space-separated for Bash 3.2 compatibility)
RUNTIME_PKGS="fastapi[all]>=0.111 supabase pydantic-settings>=2.1,<3 python-dotenv httpx anyio huggingface_hub"

# Development-only tools (space-separated for Bash 3.2 compatibility)
DEV_PKGS="ruff mypy pytest pytest-asyncio pre-commit"

# Make sure pipx is present and on PATH
python3 -m pip install --upgrade pipx >/dev/null
pipx ensurepath

echo "⏳ (Re)installing env '${ENV_NAME}' …"
# This produces the executable "uvicorn-collinearapi" on PATH
pipx install --include-deps --force "uvicorn[standard]" --suffix collinearapi

echo "⏳ Injecting runtime packages …"
pipx inject --include-apps --include-deps "${ENV_NAME}" ${RUNTIME_PKGS}

echo "⏳ Injecting dev tools …"
pipx inject --include-apps "${ENV_NAME}" ${DEV_PKGS}

mkdir -p "${BIN_DIR}"

# Create a shorter alias "collinearapi → uvicorn-collinearapi"
UVICORN_BIN=$(command -v "uvicorn-collinearapi" || true)
if [[ -n "${UVICORN_BIN}" ]]; then
  ln -sf "${UVICORN_BIN}" "${BIN_DIR}/collinearapi"
  echo "✅ Alias created: ${BIN_DIR}/collinearapi  →  uvicorn-collinearapi"
else
  echo "⚠️  Could not find 'uvicorn-collinearapi' executable — alias skipped." >&2
fi

cat <<'MSG'

🎉  Environment ready!

   • Start dev server :  collinearapi app.main:app --reload
   • Lint / format    :  ruff check app/ && ruff format app/
   • Type-check       :  mypy app/
   • Run tests        :  pytest -q

Make sure ~/.local/bin is on your PATH (re-open the terminal if needed).
MSG
