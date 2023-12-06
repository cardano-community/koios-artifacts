CREATE OR REPLACE FUNCTION grest.pool_relays()
RETURNS TABLE (
  pool_id_bech32 character varying,
  relays jsonb [],
  pool_status text
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  SELECT DISTINCT ON (pool_id_bech32)
    pool_id_bech32,
    relays,
    pool_status
  FROM grest.pool_info_cache
  ORDER BY
    pool_id_bech32,
    tx_id DESC;
END;
$$;

COMMENT ON FUNCTION grest.pool_relays IS 'A list of registered relays for all pools'; --noqa: LT01
