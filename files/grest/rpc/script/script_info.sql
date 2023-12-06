CREATE OR REPLACE FUNCTION grest.script_info(_script_hashes text [])
RETURNS TABLE (
  script_hash text,
  creation_tx_hash text,
  type text,
  value jsonb,
  bytes text,
  size word31type
)
LANGUAGE sql STABLE
AS $$
    SELECT
      ENCODE(s.hash,'hex') AS script_hash,
      ENCODE(tx.hash,'hex') AS creation_tx_hash,
      s.type::text AS type,
      s.json AS value,
      ENCODE(s.bytes,'hex')::text AS bytes,
      s.serialised_size AS size
    FROM script AS s
      INNER JOIN tx ON tx.id = s.tx_id
    WHERE s.hash IN (SELECT DECODE(s_hash, 'hex') FROM UNNEST(_script_hashes) AS s_hash)
  ;
$$;

COMMENT ON FUNCTION grest.script_info IS  'Get information about a given script FROM hashes.'; -- noqa: LT01
