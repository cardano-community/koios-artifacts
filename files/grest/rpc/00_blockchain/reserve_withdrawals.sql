CREATE OR REPLACE FUNCTION grest.reserve_withdrawals()
RETURNS TABLE (
  epoch_no word31type,
  epoch_slot word31type,
  tx_hash text,
  block_hash text,
  block_height word31type,
  amount text,
  stake_address text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    b.epoch_no,
    b.epoch_slot_no,
    ENCODE(tx.hash,'hex'),
    ENCODE(b.hash,'hex'),
    b.block_no,
    r.amount::text,
    sa.view
  FROM reserve AS r
    LEFT JOIN tx ON r.tx_id = tx.id
    INNER JOIN block AS b ON tx.block_id = b.id
    LEFT JOIN stake_address AS sa ON sa.id = r.addr_id;
$$;

COMMENT ON FUNCTION grest.reserve_withdrawals IS 'A list of withdrawals made from reserves (MIRs)'; --noqa: LT01
