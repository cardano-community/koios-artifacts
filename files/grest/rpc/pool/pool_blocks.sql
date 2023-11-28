CREATE OR REPLACE FUNCTION grest.pool_blocks(_pool_bech32 text, _epoch_no word31type DEFAULT NULL)
RETURNS TABLE (
  epoch_no word31type,
  epoch_slot word31type,
  abs_slot word63type,
  block_height word31type,
  block_hash text,
  block_time integer
)
LANGUAGE sql STABLE
AS $$
  SELECT
    b.epoch_no,
    b.epoch_slot_no AS epoch_slot,
    b.slot_no AS abs_slot,
    b.block_no AS block_height,
    ENCODE(b.hash::bytea, 'hex'),
    EXTRACT(EPOCH FROM b.time)::integer
  FROM public.block AS b
  INNER JOIN public.slot_leader AS sl ON b.slot_leader_id = sl.id
  WHERE sl.pool_hash_id = (SELECT pool_hash_id FROM grest.pool_info_cache WHERE pool_id_bech32 = _pool_bech32 ORDER BY tx_id DESC LIMIT 1)
    AND (_epoch_no IS NULL OR b.epoch_no = _epoch_no);
$$;

COMMENT ON FUNCTION grest.pool_blocks IS 'Return information about blocks minted by a given pool in current epoch (or epoch nbr if provided)'; -- noqa: LT01
