CREATE OR REPLACE FUNCTION grest.datum_info(_datum_hashes text [])
RETURNS TABLE (
  datum_hash text,
  creation_tx_hash text,
  value jsonb,
  bytes text
)
LANGUAGE plpgsql
AS $$
DECLARE
  _datum_hashes_decoded bytea[];
BEGIN
  SELECT INTO _datum_hashes_decoded ARRAY_AGG(DECODE(d_hash, 'hex'))
  FROM UNNEST(_datum_hashes) AS d_hash;
  RETURN QUERY
    SELECT
      ENCODE(d.hash,'hex'),
      ENCODE(tx.hash,'hex') AS creation_tx_hash,
      d.value,
      ENCODE(d.bytes,'hex')
    FROM datum AS d
      INNER JOIN tx ON tx.id = d.tx_id
    WHERE d.hash = ANY(_datum_hashes_decoded);
END;
$$;

COMMENT ON FUNCTION grest.datum_info IS 'Get information about a given datum FROM hashes.'; -- noqa: LT01