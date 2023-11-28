DROP VIEW IF EXISTS grest.asset_token_registry;

CREATE VIEW grest.asset_token_registry AS
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

COMMENT ON VIEW grest.asset_token_registry IS 'Get a list of assets registered via token registry on github';
