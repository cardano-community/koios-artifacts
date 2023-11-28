CREATE OR REPLACE FUNCTION grest.pool_retirements(_epoch_no numeric)
RETURNS TABLE (
  pool_id_bech32 text,
  tx_hash text,
  block_hash text,
  block_height word31type,
  epoch_no word31type,
  epoch_slot word31type,
  active_epoch_no word31type
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ph.view,
    ENCODE(tx.hash,'hex'),
    ENCODE(b.hash,'hex'),
    b.block_no,
    b.epoch_no,
    b.epoch_slot_no,
    pr.retiring_epoch
  FROM pool_retire AS pr
    LEFT JOIN tx ON pr.announced_tx_id = tx.id
    INNER JOIN block AS b ON tx.block_id = b.id
    LEFT JOIN pool_hash AS ph ON ph.id = pr.hash_id
  WHERE b.epoch_no = _epoch_no;
$$;

COMMENT ON FUNCTION grest.pool_retirements IS 'A list of all pool retirements initiated in the requested epoch'; --noqa: LT01
