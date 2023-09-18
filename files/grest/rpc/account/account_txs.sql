CREATE OR REPLACE FUNCTION grest.account_txs(_stake_address text, _after_block_height integer DEFAULT 0)
RETURNS TABLE (
  tx_hash text,
  epoch_no word31type,
  block_height word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  _tx_id_min bigint;
  _tx_id_list bigint[];
BEGIN
  SELECT INTO _tx_id_min id
    FROM tx
    WHERE block_id >= (SELECT id FROM block WHERE block_no >= _after_block_height ORDER BY id limit 1)
    ORDER BY id limit 1;

  -- all tx_out & tx_in tx ids
  SELECT INTO _tx_id_list ARRAY_AGG(tx_id)
  FROM (
    SELECT tx_id
    FROM tx_out
    WHERE stake_address_id = ANY(SELECT id FROM stake_address WHERE view = _stake_address)
      AND tx_id >= _tx_id_min
    --
    UNION
    --
    SELECT consumed_by_tx_in_id AS tx_id
    FROM tx_out
    WHERE
      tx_out.consumed_by_tx_in_id IS NULL
      AND tx_out.stake_address_id = ANY(SELECT id FROM stake_address WHERE view = _stake_address)
      AND tx_out.consumed_by_tx_in_id >= _tx_id_min
  ) AS tmp;

  RETURN QUERY
    SELECT
      DISTINCT(ENCODE(tx.hash, 'hex')) AS tx_hash,
      b.epoch_no,
      b.block_no AS block_height,
      EXTRACT(EPOCH FROM b.time)::integer AS block_time
    FROM public.tx
    INNER JOIN public.block AS b ON b.id = tx.block_id
    WHERE tx.id = ANY(_tx_id_list)
      AND b.block_no >= _after_block_height
    ORDER BY b.block_no DESC;
END;
$$;

COMMENT ON FUNCTION grest.account_txs IS 'Get transactions associated with a given stake address'; -- noqa: LT01
