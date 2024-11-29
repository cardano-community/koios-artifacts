CREATE OR REPLACE FUNCTION grest.address_txs(_addresses text [], _after_block_height integer DEFAULT 0)
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

  -- all tx_out & tx_in tx ids
  SELECT INTO _tx_id_list ARRAY_AGG(tx_id)
  FROM (
    SELECT tx_id
    FROM tx_out
    INNER JOIN address AS a on a.id = tx_out.address_id
    WHERE a.address = ANY (_addresses)
      AND tx_id >= _tx_id_min
    --
    UNION
    --
    SELECT consumed_by_tx_id
    FROM tx_out
    INNER JOIN address AS a ON a.id = tx_out.address_id
    WHERE tx_out.consumed_by_tx_id IS NOT NULL
      AND a.address = ANY(_addresses)
      AND tx_out.consumed_by_tx_id >= _tx_id_min
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
    ORDER BY b.block_no DESC;
END;
$$;

COMMENT ON FUNCTION grest.address_txs IS 'Get the transaction hash list of a Cardano address array, optionally filtering after specified block height (inclusive).'; -- noqa: LT01
