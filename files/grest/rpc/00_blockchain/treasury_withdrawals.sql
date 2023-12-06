CREATE OR REPLACE FUNCTION grest.treasury_withdrawals()
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
    t.amount::text,
    sa.view
  FROM treasury AS t
    LEFT JOIN tx ON t.tx_id = tx.id
    INNER JOIN block AS b ON tx.block_id = b.id
    LEFT JOIN stake_address AS sa ON sa.id = t.addr_id;
$$;

COMMENT ON FUNCTION grest.treasury_withdrawals IS 'A list of withdrawals made from treasury'; --noqa: LT01
