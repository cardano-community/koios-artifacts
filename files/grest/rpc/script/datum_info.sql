CREATE OR REPLACE FUNCTION grest.datum_info(_datum_hashes text [])
RETURNS TABLE (
  datum_hash text,
  creation_tx_hash text,
  value jsonb,
  bytes text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(d.hash,'hex'),
    ENCODE(tx.hash,'hex') AS creation_tx_hash,
    d.value,
    ENCODE(d.bytes,'hex')
  FROM datum AS d
    INNER JOIN tx ON tx.id = d.tx_id
  WHERE d.hash IN (SELECT DECODE(d_hash, 'hex') FROM UNNEST(_datum_hashes) AS d_hash);
$$;

COMMENT ON FUNCTION grest.datum_info IS 'Get information about a given datum FROM hashes.'; -- noqa: LT01
