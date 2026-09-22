-- 0001_grest_owner_role.sql
-- Creates the least-privilege role used by koios-pool-meta-fetcher to write
-- to grest-owned tables. Idempotent.
--
-- This migration is independent of {schema}: the role is cluster-scoped.

DO $pmf$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grest_owner') THEN
    CREATE ROLE grest_owner NOLOGIN;
    RAISE NOTICE 'role grest_owner created';
  END IF;
END
$pmf$;
