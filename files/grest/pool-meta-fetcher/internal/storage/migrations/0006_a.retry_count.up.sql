-- 0006_a.retry_count.up.sql
-- Adds the columns needed for exponential-backoff retry on failed pools.
--
-- consecutive_failures: count of consecutive failed attempts for this pool,
-- incremented on each failure and reset to 0 on success.
--
-- next_attempt_at: when the daemon is next allowed to refetch this pool.
-- Null when the pool is in the "successful" bucket (driven by poll_interval).
-- The daemon sets it to now() + (failed_pool_retry_seconds * 2^(consecutive_failures - 1))
-- after each failure, capped at 2^10 * base (= roughly 17h at the default 60s base).

ALTER TABLE {schema}.pool_offchain_metadata
  ADD COLUMN IF NOT EXISTS consecutive_failures INT          NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS next_attempt_at       TIMESTAMPTZ;

-- Schedule semantics:
--   next_attempt_at := now() + base * 2^consecutive_failures (in seconds)
--   base = fetch.poll.failed_pool_retry_seconds (default 60)
--   consecutive_failures is incremented atomically with this UPDATE
--   (so the formula reads the PRE-increment value via the SET-clause view
--   of the row).
-- So:
--   1st failure: count goes 0 -> 1, next_attempt_at = now + 60 * 2^0 = +60s
--   2nd failure: count goes 1 -> 2, next_attempt_at = now + 60 * 2^1 = +120s
--   3rd failure: count goes 2 -> 3, next_attempt_at = now + 60 * 2^2 = +240s
--   ...
-- Capped at 2^10 = +1024 * base seconds (roughly 17h at default 60s base).
-- On any successful fetch, UpsertBatch resets count to 0 and
-- next_attempt_at to NULL.

COMMENT ON COLUMN {schema}.pool_offchain_metadata.consecutive_failures
  IS 'Count of consecutive failed attempts; reset on success; used for exponential backoff';
COMMENT ON COLUMN {schema}.pool_offchain_metadata.next_attempt_at
  IS 'Earliest wall-clock time the daemon may retry; NULL on success';

-- Used by discovery: WHERE next_attempt_at <= now(). Partial index because
-- only failed pools have a non-null next_attempt_at.
CREATE INDEX IF NOT EXISTS idx_pool_offchain_metadata_next_attempt_at
  ON {schema}.pool_offchain_metadata (next_attempt_at)
  WHERE next_attempt_at IS NOT NULL;
