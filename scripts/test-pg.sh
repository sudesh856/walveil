#!/usr/bin/env bash
set -euo pipefail

echo "==> Starting test environment..."
docker compose up -d postgres redis webhook-receiver

echo "==> Waiting for Postgres to be ready..."
until docker compose exec postgres pg_isready -U walveil; do
  sleep 1
done

echo "==> Setting up publication and replication user..."
docker compose exec postgres psql -U walveil -d walveil <<'SQL'
-- Create test tables
CREATE TABLE IF NOT EXISTS public.orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status TEXT NOT NULL DEFAULT 'pending',
  total NUMERIC(10,2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS public.users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL,
  password_hash TEXT NOT NULL
);

-- Full replica identity so UPDATE/DELETE carry old values
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.users REPLICA IDENTITY FULL;

-- Publication
CREATE PUBLICATION walveil_pub FOR TABLE public.orders, public.users;

-- Replication user (least privilege — NOT superuser)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'walveil_repl') THEN
    CREATE ROLE walveil_repl WITH REPLICATION LOGIN PASSWORD 'replpassword';
  END IF;
END
$$;

GRANT SELECT ON public.orders TO walveil_repl;
GRANT SELECT ON public.users TO walveil_repl;
SQL

echo "==> Postgres ready. Run 'go test ./...' to execute integration tests."