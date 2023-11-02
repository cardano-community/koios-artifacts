CREATE OR REPLACE FUNCTION grest.tx_metadata(_tx_hashes text [])
RETURNS TABLE (
  tx_hash text,
  metadata jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT
    t1.tx_hash,
    metadata_t.metadata
  FROM (
    SELECT
      tx.id,
      ENCODE(tx.hash, 'hex') AS tx_hash
    FROM
      public.tx
    WHERE
      tx.hash::bytea = ANY(
        SELECT
          DECODE(hashes, 'hex')
        FROM
          UNNEST(_tx_hashes) AS hashes
      )
  ) AS t1
  LEFT JOIN LATERAL (
    SELECT
      JSONB_OBJECT_AGG(
        tx_metadata.key::text,
        tx_metadata.json
      ) AS metadata
    FROM
      tx_metadata
    WHERE
      tx_id = t1.id
  ) AS metadata_t ON TRUE;
END;
$$;

COMMENT ON FUNCTION grest.tx_metadata IS 'Get transaction metadata.'; -- noqa: LT01
