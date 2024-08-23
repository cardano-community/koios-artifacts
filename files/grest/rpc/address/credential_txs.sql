CREATE OR REPLACE FUNCTION grest.credential_txs(_payment_credentials text [], _after_block_height integer DEFAULT 0)
RETURNS TABLE (
  tx_hash text,
  epoch_no word31type,
  block_height word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  _payment_cred_bytea  bytea[];
  _tx_id_min bigint;
  _tx_id_list     bigint[];
BEGIN
  -- convert input _payment_credentials array into bytea array
  SELECT INTO _payment_cred_bytea ARRAY_AGG(cred_bytea)
  FROM (
    SELECT DECODE(cred_hex, 'hex') AS cred_bytea
    FROM UNNEST(_payment_credentials) AS cred_hex
  ) AS tmp;

  SELECT INTO _tx_id_min id
    FROM tx
    WHERE block_id >= (SELECT id FROM block WHERE block_no >= _after_block_height ORDER BY id limit 1)
    ORDER BY id limit 1;

  -- all tx_out & tx_in tx ids
  SELECT INTO _tx_id_list ARRAY_AGG(tx_id)
  FROM (
    SELECT tx_id
    FROM tx_out
    WHERE payment_cred = ANY(_payment_cred_bytea)
      AND tx_id >= _tx_id_min
    --
    UNION
    --
    SELECT consumed_by_tx_id AS tx_id
    FROM tx_out
    WHERE tx_out.consumed_by_tx_id IS NOT NULL
      AND tx_out.payment_cred = ANY(_payment_cred_bytea)
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

COMMENT ON FUNCTION grest.credential_txs IS 'Get the transaction hash list of a payment credentials array, optionally filtering after specified block height (inclusive).'; --noqa: LT01
