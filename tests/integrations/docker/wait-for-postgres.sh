#!/bin/bash

cmd="$@"

until nc -z "$STAT_DATABASE_HOST" 5432; do
  echo "Postgres is unavailable - sleeping"
  sleep 2
done

echo "Postgres is up - executing command"

exec $cmd
