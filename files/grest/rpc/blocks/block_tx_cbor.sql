CREATE OR REPLACE FUNCTION grest.block_tx_cbor(_block_hashes text [])
RETURNS TABLE (
  tx_hash text,
  block_hash text,
  block_height word31type,
  epoch_no word31type,
  absolute_slot word63type,
  tx_timestamp integer,
  cbor text
)
LANGUAGE sql STABLE
AS $$
  SELECT *
  FROM grest.tx_cbor(
      (
        SELECT ARRAY_AGG(ENCODE(hash, 'hex'))
        FROM  tx
        WHERE tx.block_id = ANY(
          SELECT id
          FROM block
          WHERE hash = ANY(
            SELECT DISTINCT DECODE(hashes_hex, 'hex')
            FROM UNNEST(_block_hashes) AS hashes_hex
          )
        )
      )
    );
$$;

COMMENT ON FUNCTION grest.block_tx_cbor IS 'Get Raw transaction in CBOR format for given block hashes.'; -- noqa: LT01
