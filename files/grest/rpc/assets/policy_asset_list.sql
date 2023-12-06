CREATE OR REPLACE FUNCTION grest.policy_asset_list(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  fingerprint varchar,
  total_supply text,
  decimals integer
)
LANGUAGE sql STABLE
AS $$
    SELECT
      ENCODE(ma.name, 'hex') AS asset_name,
      ma.fingerprint AS fingerprint,
      aic.total_supply::text,
      aic.decimals
    FROM multi_asset AS ma
    INNER JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
    WHERE ma.policy = DECODE(_asset_policy, 'hex');
$$;

COMMENT ON FUNCTION grest.policy_asset_list IS 'Get a list of all asset under a policy'; -- noqa: LT01
