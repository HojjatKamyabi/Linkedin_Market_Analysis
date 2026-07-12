#!/bin/bash
set -e

# When the Compose PostgreSQL service initializes a fresh volume, create the application database.
# The standard PostgreSQL image already creates POSTGRES_USER and POSTGRES_DB.
# This script creates only the additional application database.

APP_DB="linkedin_market"

echo "Checking if database $APP_DB exists..."
if psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='$APP_DB'" | grep -q 1; then
    echo "Database $APP_DB already exists."
else
    echo "Creating database $APP_DB..."
    createdb -U "$POSTGRES_USER" -O "$POSTGRES_USER" "$APP_DB"
    echo "Database $APP_DB created successfully."
    
    echo "Granting privileges on $APP_DB to $POSTGRES_USER..."
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$APP_DB" <<-EOSQL
        GRANT CONNECT ON DATABASE $APP_DB TO "$POSTGRES_USER";
        GRANT USAGE, CREATE ON SCHEMA public TO "$POSTGRES_USER";
EOSQL
    echo "Privileges granted."
fi
