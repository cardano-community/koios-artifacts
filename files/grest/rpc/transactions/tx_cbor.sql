CREATE OR REPLACE FUNCTION grest.tx_cbor(_tx_hashes text [])
RETURNS TABLE (
  tx_hash text,
  cbor text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(tx.hash::bytea, 'hex') AS tx_hash,
    ENCODE(tx_cbor.bytes::bytea, 'hex') AS tx_cbor
  FROM public.tx
    LEFT JOIN public.tx_cbor ON tx.id = tx_cbor.tx_id
  WHERE tx.hash::bytea = ANY(
      SELECT
        DECODE(hashes, 'hex')
      FROM
        UNNEST(_tx_hashes) AS hashes
    )
  ORDER BY tx.id;
$$;

COMMENT ON FUNCTION grest.tx_cbor IS 'Get raw transaction(s) in CBOR format'; -- noqa: LT01
