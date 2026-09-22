-- 0001_create_schema.up.sql
-- Creates the schema that the daemon will use, defaulting to grest and
-- overridable via the schema_name config option.
--
-- This migration is the bootstrap: subsequent migrations reference this
-- schema (after {schema} substitution).

CREATE SCHEMA IF NOT EXISTS {schema};

COMMENT ON SCHEMA {schema} IS 'Schema for koios-pool-meta-fetcher objects; defaults to grest';
