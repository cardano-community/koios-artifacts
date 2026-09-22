-- koios-pool-meta-fetcher cache (populated externally by the Go daemon, NOT by db-sync).
-- Stores the *current* raw off-chain metadata for every Cardano stake pool.
-- See openspec/specs/pool-offchain-metadata-cache/spec.md for the contract.

CREATE TABLE IF NOT EXISTS grest.pool_offchain_metadata (
  id                    BIGSERIAL    PRIMARY KEY,
  pool_id               BIGINT       NOT NULL UNIQUE REFERENCES public.pool_hash(id) ON DELETE CASCADE,
  meta_url              VARCHAR(256) NOT NULL,
  meta_hash             BYTEA        CHECK (meta_hash IS NULL OR OCTET_LENGTH(meta_hash) = 32),
  meta_json             JSONB,
  last_polled           TIMESTAMPTZ  NOT NULL,
  last_pool_update_id   BIGINT       NOT NULL REFERENCES public.pool_update(id) ON DELETE CASCADE,
  is_valid              BOOLEAN      NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE  grest.pool_offchain_metadata                          IS 'Current parsed off-chain metadata per pool, populated by koios-pool-meta-fetcher';
COMMENT ON COLUMN grest.pool_offchain_metadata.id                       IS 'Surrogate primary key';
COMMENT ON COLUMN grest.pool_offchain_metadata.pool_id                  IS 'FK -> public.pool_hash(id); UNIQUE enforces one row per pool';
COMMENT ON COLUMN grest.pool_offchain_metadata.meta_url                 IS 'Last URL the daemon actually fetched (set on every attempt, success or failure)';
COMMENT ON COLUMN grest.pool_offchain_metadata.meta_hash                IS 'Blake2b-256 of the raw response body at fetch time (NULL for rows that have never had a successful fetch; informational; no RPC verifies against pmr.hash)';
COMMENT ON COLUMN grest.pool_offchain_metadata.meta_json                IS 'Parsed payload round-tripped through json.Marshal (NULL for rows that have never had a successful fetch; what RPCs return)';
COMMENT ON COLUMN grest.pool_offchain_metadata.last_polled              IS 'Last successful or attempted fetch time';
COMMENT ON COLUMN grest.pool_offchain_metadata.last_pool_update_id      IS 'FK -> public.pool_update(id); provenance pointer to the chain cert whose declared URL we fetched';
COMMENT ON COLUMN grest.pool_offchain_metadata.is_valid                 IS 'FALSE if the row exists solely to track a failed first attempt (meta_hash/meta_json are NULL); TRUE once a successful fetch has populated the row';

-- Indexes -----------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_pool_offchain_metadata_last_polled
  ON grest.pool_offchain_metadata (last_polled DESC);

CREATE INDEX IF NOT EXISTS idx_pool_offchain_metadata_last_pool_update_id
  ON grest.pool_offchain_metadata (last_pool_update_id);

CREATE INDEX IF NOT EXISTS idx_pool_offchain_metadata_is_valid_false
  ON grest.pool_offchain_metadata (is_valid)
  WHERE is_valid = FALSE;

-- Access control ---------------------------------------------------------------
-- (Role 'grest_owner' is created in db-scripts/basics.sql.)

ALTER TABLE grest.pool_offchain_metadata OWNER TO grest_owner;

GRANT SELECT ON grest.pool_offchain_metadata TO web_anon;

GRANT INSERT, UPDATE, DELETE ON grest.pool_offchain_metadata TO grest_owner;
GRANT USAGE   ON SEQUENCE grest.pool_offchain_metadata_id_seq TO grest_owner;

-- populated by koios-pool-meta-fetcher (external daemon)
