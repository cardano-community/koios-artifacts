CREATE OR REPLACE FUNCTION grest.native_script_list()
RETURNS TABLE (
  script_hash text,
  creation_tx_hash text,
  type text,
  size word31type
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(s.hash, 'hex')::text AS script_hash,
    ENCODE(tx.hash, 'hex')::text AS creation_tx_hash,
    s.type::text AS type,
    s.serialised_size AS size
  FROM script AS s
    INNER JOIN tx ON tx.id = s.tx_id
  WHERE s.type IN ('timelock', 'multisig');
$$;

COMMENT ON FUNCTION grest.native_script_list IS 'Get a list of all native(multisig/timelock) script hashes with creation tx hash, type and script size.'; --noqa: LT01
