CREATE OR REPLACE FUNCTION grest.block_txs(_block_hashes text [])
RETURNS TABLE (
  block_hash text,
  tx_hash text,
  epoch_no word31type,
  block_height word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  _block_hashes_bytea bytea[];
  _block_ids integer[];
BEGIN
  SELECT INTO _block_hashes_bytea ARRAY_AGG(block_hashes_bytea)
  FROM (
    SELECT DECODE(hex, 'hex') AS block_hashes_bytea
    FROM UNNEST(_block_hashes) AS hex
  ) AS tmp;

  SELECT INTO _block_ids ARRAY_AGG(b.id)
  FROM public.block AS b
  WHERE b.hash = ANY(_block_hashes_bytea);

  RETURN QUERY
    SELECT
      ENCODE(b.hash, 'hex'),
      ENCODE(tx.hash, 'hex') AS tx_hash,
      b.epoch_no,
      b.block_no AS block_height,
      EXTRACT(EPOCH FROM b.time)::integer AS block_time
    FROM public.block AS b
    INNER JOIN public.tx ON tx.block_id = b.id
    WHERE b.id = ANY(_block_ids)
  ;
END;
$$;

COMMENT ON FUNCTION grest.block_txs IS 'Get all transactions contained in given blocks'; -- noqa: LT01
