CREATE OR REPLACE FUNCTION grest.pool_registrations(_epoch_no numeric)
RETURNS TABLE (
  pool_id_bech32 text,
  tx_hash text,
  block_hash text,
  block_height word31type,
  epoch_no word31type,
  epoch_slot word31type,
  active_epoch_no bigint
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
    pu.active_epoch_no
  FROM pool_update AS pu
    LEFT JOIN tx ON pu.registered_tx_id = tx.id
    INNER JOIN block AS b ON tx.block_id = b.id
    LEFT JOIN pool_hash AS ph ON ph.id = pu.hash_id
  WHERE b.epoch_no = _epoch_no;
$$;

COMMENT ON FUNCTION grest.pool_registrations IS 'A list of all pool registrations initiated in the requested epoch'; --noqa: LT01
