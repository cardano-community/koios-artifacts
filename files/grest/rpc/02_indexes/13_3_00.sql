CREATE INDEX IF NOT EXISTS pool_stat_pool_hash_id ON pool_stat(pool_hash_id);
CREATE INDEX IF NOT EXISTS pool_stat_epoch_no ON pool_stat(epoch_no);
CREATE INDEX IF NOT EXISTS idx_drep_hash_view ON drep_hash (view);