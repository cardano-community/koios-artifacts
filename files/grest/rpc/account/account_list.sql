CREATE OR REPLACE FUNCTION grest.account_list()
RETURNS TABLE (
  stake_address text,
  stake_address_hex text,
  script_hash text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    grest.cip5_hex_to_stake_addr(sa.hash_raw),
    ENCODE(sa.hash_raw,'hex'),
    ENCODE(sa.script_hash,'hex')
  FROM stake_address AS sa
  ORDER BY sa.id;
$$;

COMMENT ON FUNCTION grest.account_list IS 'Get a list of all accounts'; -- noqa: LT01
