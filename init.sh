#!/bin/bash

echo 'Waiting for postgres...'
echo "POSTGRES_HOST: $POSTGRES_HOST"
while ! pg_isready -h "$POSTGRES_HOST" -p 5432 > /dev/null 2>&1; do
    sleep 1
done
echo 'PostgreSQL is ready!'

# Execute the SQL script
export PGPASSWORD="$POSTGRES_PASSWORD"
if psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/initial.sql; then
    echo 'SQL script executed successfully.'
else
    echo 'Failed to execute SQL script.' >&2
    exit 1
fi