-- db-sync dijkstra-0-0 stores each Dijkstra sub-transaction (CIP-118) as a tx row pointing at its top-level
-- transaction, but adds no index on that link. Most rows are top-level, so a partial index stays small.
CREATE INDEX IF NOT EXISTS idx_tx_parent_tx_id ON public.tx USING btree (parent_tx_id) WHERE parent_tx_id IS NOT NULL;
