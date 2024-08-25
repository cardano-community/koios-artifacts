CREATE INDEX IF NOT EXISTS pool_stat_pool_hash_id ON pool_stat(pool_hash_id);
CREATE INDEX IF NOT EXISTS pool_stat_epoch_no ON pool_stat(epoch_no);
-- CREATE INDEX IF NOT EXISTS idx_drep_hash_view ON drep_hash (view);
CREATE INDEX IF NOT EXISTS idx_drep_hash_raw ON drep_hash (raw);
CREATE INDEX IF NOT EXISTS idx_reward_rest_addr_id ON reward_rest (addr_id);
CREATE INDEX IF NOT EXISTS idx_reward_rest_spendable_epoch ON reward_rest (spendable_epoch);