#!/bin/bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
    echo "Usage: $0 [--force]" >&2
    exit 1
fi

FORCE=0
if [ "${1:-}" == "--force" ]; then
    FORCE=1
elif [ "${1:-}" != "" ]; then
    echo "Usage: $0 [--force]" >&2
    exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(dirname "$SCRIPT_DIR")
ENV_PATH="$REPO_ROOT/.env"
ENV_EXAMPLE_PATH="$REPO_ROOT/.env.example"

if [ -f "$ENV_PATH" ] && [ $FORCE -eq 0 ]; then
    echo ".env already exists. Use --force to overwrite." >&2
    exit 1
fi

cp "$ENV_EXAMPLE_PATH" "$ENV_PATH"

generate_safe_password() {
    if command -v python3 >/dev/null 2>&1; then
        python3 -c "import secrets, string; print(''.join(secrets.choice(string.ascii_letters + string.digits + '-_') for _ in range(24)))"
    elif command -v openssl >/dev/null 2>&1; then
        RAW_HEX=$(openssl rand -hex 24)
        echo "${RAW_HEX:0:24}"
    else
        echo "Error: Need openssl or python3 to generate password" >&2
        exit 1
    fi
}

generate_fernet_key() {
    if command -v python3 >/dev/null 2>&1; then
        python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
    elif command -v openssl >/dev/null 2>&1; then
        openssl rand -base64 32 | tr -d '\n' | tr '+' '-' | tr '/' '_'
    else
        echo "Error: Need python3 or openssl to generate Fernet key" >&2
        exit 1
    fi
}

DB_PASS=$(generate_safe_password)
FERNET=$(generate_fernet_key)
ADMIN_PASS=$(generate_safe_password)

if command -v python3 >/dev/null 2>&1; then
    python3 -c "
import sys
with open('$ENV_PATH', 'r') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if line.startswith('DB_PASSWORD='): lines[i] = 'DB_PASSWORD=' + sys.argv[1] + '\n'
    elif line.startswith('DB_PASSWORD_URI='): lines[i] = 'DB_PASSWORD_URI=' + sys.argv[1] + '\n'
    elif line.startswith('FERNET_KEY='): lines[i] = 'FERNET_KEY=' + sys.argv[2] + '\n'
    elif line.startswith('AIRFLOW_ADMIN_PASSWORD='): lines[i] = 'AIRFLOW_ADMIN_PASSWORD=' + sys.argv[3] + '\n'
with open('$ENV_PATH', 'w') as f:
    f.writelines(lines)
" "$DB_PASS" "$FERNET" "$ADMIN_PASS"
else
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s/^DB_PASSWORD=$/DB_PASSWORD=${DB_PASS}/" "$ENV_PATH"
        sed -i '' "s/^DB_PASSWORD_URI=$/DB_PASSWORD_URI=${DB_PASS}/" "$ENV_PATH"
        sed -i '' "s/^FERNET_KEY=$/FERNET_KEY=${FERNET}/" "$ENV_PATH"
        sed -i '' "s/^AIRFLOW_ADMIN_PASSWORD=$/AIRFLOW_ADMIN_PASSWORD=${ADMIN_PASS}/" "$ENV_PATH"
    else
        sed -i "s/^DB_PASSWORD=$/DB_PASSWORD=${DB_PASS}/" "$ENV_PATH"
        sed -i "s/^DB_PASSWORD_URI=$/DB_PASSWORD_URI=${DB_PASS}/" "$ENV_PATH"
        sed -i "s/^FERNET_KEY=$/FERNET_KEY=${FERNET}/" "$ENV_PATH"
        sed -i "s/^AIRFLOW_ADMIN_PASSWORD=$/AIRFLOW_ADMIN_PASSWORD=${ADMIN_PASS}/" "$ENV_PATH"
    fi
fi

mkdir -p "$REPO_ROOT/data/raw" "$REPO_ROOT/data/processed" "$REPO_ROOT/airflow/logs" "$REPO_ROOT/airflow/plugins"

echo "Created .env"
echo "Generated local database password: true"
echo "Generated Fernet key: true"
echo "Generated Airflow admin password: true"
echo "LLM_API_KEY still requires user input"
