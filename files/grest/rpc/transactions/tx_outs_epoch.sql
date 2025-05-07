CREATE OR REPLACE FUNCTION grest.tx_outs_epoch(_epoch_no numeric)
RETURNS TABLE (
  tx_hash text,
  tx_index smallint,
  address text,
  stake_address text,
  payment_cred text,
  epoch_no word31type,
  block_height word31type,
  block_time integer,
  value lovelace,
  datum_hash text,
  is_spent boolean
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(tx.hash, 'hex')::text AS tx_hash,
    txo.index::smallint AS tx_index,
    a.address::text,
    sa.view::text AS stake_address,
    ENCODE(a.payment_cred, 'hex') AS payment_cred,
    b.epoch_no,
    b.block_no AS block_height,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    txo.value,
    ENCODE(txo.data_hash, 'hex') AS datum_hash,
    txo.consumed_by_tx_id IS NOT NULL AS is_spent
  FROM public.tx_out AS txo
    INNER JOIN tx ON tx.id = txo.tx_id
    INNER JOIN public.address AS a ON a.id = txo.address_id
    LEFT JOIN public.stake_address AS sa ON sa.id = txo.stake_address_id
    INNER JOIN public.block AS b ON b.id = tx.block_id
  WHERE b.epoch_no = _epoch_no::word31type
  ORDER BY txo.id;
$$;

COMMENT ON FUNCTION grest.tx_outs_epoch IS 'Get a list of transaction outputs with basic details for requested epoch'; -- noqa: LT01
