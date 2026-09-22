-- 0005_a.last_polled_outcome.up.sql
-- Adds per-pool "last outcome" timestamps to drive dual-cadence retries:
-- successful pools get re-checked every poll_interval (default 5 days),
-- failed pools get re-checked every failed_pool_retry_seconds (default 60s).
--
-- Existing rows get last_succeeded_polled_at = 'epoch' (treated as
-- last-attempt-was-success), so pre-existing populated rows stay in the
-- "5-day cycle" bucket rather than being treated as failing. New failed
-- attempts overwrite last_failed_polled_at; new successes overwrite both
-- fields.

ALTER TABLE {schema}.pool_offchain_metadata
  ADD COLUMN IF NOT EXISTS last_succeeded_polled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS last_failed_polled_at    TIMESTAMPTZ;

COMMENT ON COLUMN {schema}.pool_offchain_metadata.last_succeeded_polled_at
  IS 'TIMESTAMP of the most recent successful fetch; defaults to row creation';
COMMENT ON COLUMN {schema}.pool_offchain_metadata.last_failed_polled_at
  IS 'TIMESTAMP of the most recent failed fetch (NULL if no failures yet)';

CREATE INDEX IF NOT EXISTS idx_pool_offchain_metadata_last_succeeded_at
  ON {schema}.pool_offchain_metadata (last_succeeded_polled_at);

CREATE INDEX IF NOT EXISTS idx_pool_offchain_metadata_last_failed_at
  ON {schema}.pool_offchain_metadata (last_failed_polled_at)
  WHERE last_failed_polled_at IS NOT NULL;
