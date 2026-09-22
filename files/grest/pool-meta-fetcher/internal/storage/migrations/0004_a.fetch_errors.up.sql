-- 0003_b.fetch_errors.up.sql
-- Stores an audit trail of every fetch failure (HTTP errors, timeouts,
-- size violations, JSON validation failures, hash mismatches if opted in,
-- IP blocks, etc.). Lets ops answer "why is this pool missing?" without
-- scraping logs.

CREATE TABLE IF NOT EXISTS {schema}.pool_offchain_fetch_error (
  id                  BIGSERIAL    PRIMARY KEY,
  pool_id             BIGINT       NOT NULL REFERENCES public.pool_hash(id) ON DELETE CASCADE,
  pmr_id              BIGINT       NOT NULL,                                         -- on-chain pool_metadata_ref.id at fetch time; plain INT, no FK (deliberately decoupled from db-sync pruning)
  meta_url            VARCHAR(256) NOT NULL,                                         -- snapshot; not FK (URL on-chain may change later)
  http_status_code    INT,                       -- nullable: DNS failure / TLS fail don't have one
  error_class         VARCHAR(64)  NOT NULL,      -- 'timeout','ip_blocked','size_violation','content_type',...
  error_message       TEXT         NOT NULL,      -- human-readable detail
  response_preview    BYTEA,                     -- up to ~4 KiB of the body for diagnosis
  attempted_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE  {schema}.pool_offchain_fetch_error IS 'Audit log of per-pool fetch failures; TTL-driven cleanup recommended';
COMMENT ON COLUMN {schema}.pool_offchain_fetch_error.pool_id        IS 'FK -> public.pool_hash(id); CASCADE removes audit rows when the pool is deregistered from db-sync';
COMMENT ON COLUMN {schema}.pool_offchain_fetch_error.pmr_id         IS 'public.pool_metadata_ref.id at fetch time (snapshot, intentionally no FK — audit history must survive db-sync pruning of the on-chain metadata ref)';
COMMENT ON COLUMN {schema}.pool_offchain_fetch_error.meta_url       IS 'Snapshot of the URL attempted at fetch time (intentionally not a FK; the chain URL may change later)';
COMMENT ON COLUMN {schema}.pool_offchain_fetch_error.error_class   IS 'Stable error category (timeout, ip_blocked, content_type, size, json, hash, etc)';
COMMENT ON COLUMN {schema}.pool_offchain_fetch_error.http_status_code IS 'HTTP status code if reached; NULL for connect/DNS/TLS errors';
COMMENT ON COLUMN {schema}.pool_offchain_fetch_error.response_preview IS 'First ~4KB of body; NULL for connect errors';

-- Operational indexes. Most queries will be ordered by time.

CREATE INDEX IF NOT EXISTS idx_pool_offchain_fetch_error_pool_id
  ON {schema}.pool_offchain_fetch_error (pool_id, attempted_at DESC);

CREATE INDEX IF NOT EXISTS idx_pool_offchain_fetch_error_attempted_at
  ON {schema}.pool_offchain_fetch_error (attempted_at DESC);

CREATE INDEX IF NOT EXISTS idx_pool_offchain_fetch_error_class
  ON {schema}.pool_offchain_fetch_error (error_class, attempted_at DESC);

ALTER TABLE {schema}.pool_offchain_fetch_error OWNER TO grest_owner;

GRANT SELECT ON {schema}.pool_offchain_fetch_error TO web_anon;

GRANT INSERT, UPDATE, DELETE ON {schema}.pool_offchain_fetch_error TO grest_owner;
GRANT USAGE   ON SEQUENCE {schema}.pool_offchain_fetch_error_id_seq TO grest_owner;
