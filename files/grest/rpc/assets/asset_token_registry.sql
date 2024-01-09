CREATE OR REPLACE FUNCTION grest.asset_token_registry()
RETURNS TABLE (
  policy_id text,
  asset_name text,
  asset_name_ascii text,
  ticker text,
  description text,
  url text,
  decimals integer,
  logo text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    asset_policy AS policy_id,
    asset_name,
    name AS asset_name_ascii,
    ticker,
    description,
    url,
    decimals,
    logo
  FROM grest.asset_registry_cache;
$$;

COMMENT ON FUNCTION grest.asset_token_registry IS 'An array of token registry information (registered via github) for each asset';
