#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="uvicorncollinearapi"      
BIN_DIR="${HOME}/.local/bin"      

RUNTIME_PKGS=(
  "fastapi[all]>=0.111"
  supabase
  pydantic-settings
  python-dotenv
  httpx
  anyio
  huggingface_hub
  psycopg[binary]   
  alembic         
)

DEV_PKGS=(
  ruff
  mypy
  pytest
  pytest-asyncio
  pre-commit
)

python3 -m pip install --upgrade pipx >/dev/null
pipx ensurepath


echo "(Re)installing env '${ENV_NAME}' …"
pipx install --include-deps --force "uvicorn[standard]" --suffix collinearapi

echo "Injecting runtime packages …"
pipx inject --include-apps --include-deps "${ENV_NAME}" "${RUNTIME_PKGS[@]}"
echo "Injecting dev tools …"
pipx inject --include-apps "${ENV_NAME}" "${DEV_PKGS[@]}"
mkdir -p "${BIN_DIR}"

UVICORN_BIN=$(command -v "${ENV_NAME}" || true)
if [[ -n "${UVICORN_BIN}" ]]; then
  ln -sf "${UVICORN_BIN}" "${BIN_DIR}/collinearapi"
  echo "Alias created: ${BIN_DIR}/collinearapi  →  ${ENV_NAME}"
else
  echo "Could not find '${ENV_NAME}' executable — alias skipped." >&2
fi

cat > "${BIN_DIR}/alembic" <<'EOF'
#!/usr/bin/env bash
# Always run the Alembic that lives in the project’s pipx venv
exec pipx run --spec alembic==latest alembic "$@"
EOF
chmod +x "${BIN_DIR}/alembic"
echo "Wrapper created: ${BIN_DIR}/alembic"

echo "   • Start dev server :  collinearapi app.main:app --reload"
echo "   • DB migrations    :  alembic revision --autogenerate -m '…'"
echo "                          alembic upgrade head"
echo "   • Lint / format    :  ruff check app/ && ruff format app/"
echo "   • Type-check       :  mypy app/"
echo "   • Run tests        :  pytest -q"

