CREATE OR REPLACE FUNCTION grest.address_outputs(_addresses text [], _after_block_height integer DEFAULT 0)
RETURNS TABLE (
  address text,
  tx_hash text,
  tx_index smallint,
  value text,
  stake_address text,
  payment_cred text,
  epoch_no word31type,
  block_height word31type,
  block_time integer,
  is_spent boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
  _tx_id_min bigint;
BEGIN
  SELECT INTO _tx_id_min min(id)
  FROM tx 
  WHERE block_id = (
    SELECT id
    FROM block AS b
    WHERE block_no >= _after_block_height 
      AND EXISTS (
        SELECT true
           FROM tx t 
           WHERE t.block_id = b.id
      )
    ORDER BY id LIMIT 1
  );

  RETURN QUERY
    SELECT
      a.address::text,
      ENCODE(tx.hash, 'hex') AS tx_hash,
      txo.index::smallint AS tx_index,
      txo.value::text,
      sa.view::text AS stake_address,
      ENCODE(a.payment_cred, 'hex') AS payment_cred,
      b.epoch_no,
      b.block_no AS block_height,
      EXTRACT(EPOCH FROM b.time)::integer AS block_time,
      CASE
        WHEN txo.consumed_by_tx_id IS NULL THEN false
        ELSE true
      END AS is_spent
    FROM public.address AS a
      LEFT JOIN public.stake_address AS sa ON sa.id = a.stake_address_id
      INNER JOIN public.tx_out AS txo ON a.id = txo.address_id
      INNER JOIN public.tx ON tx.id = txo.tx_id
      INNER JOIN public.block AS b ON b.id = tx.block_id
    WHERE a.address = ANY(_addresses)
      AND tx.id > _tx_id_min
    ORDER BY b.block_no DESC
    ;
END;
$$;

COMMENT ON FUNCTION grest.address_outputs IS 'Get a basic information about transaction outputs for a given address array, optionally filtering after specified block height (inclusive).'; -- noqa: LT01
