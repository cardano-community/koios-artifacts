DROP INDEX IF EXISTS idx_stake_address_view;
CREATE INDEX IF NOT EXISTS idx_stake_address_hash_raw ON stake_address (hash_raw);
CREATE INDEX IF NOT EXISTS idx_drep_hash_raw ON drep_hash (raw);
