CREATE OR REPLACE FUNCTION grest.policy_asset_list(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  fingerprint varchar,
  total_supply text,
  decimals integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_policy_decoded bytea;
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  RETURN QUERY
    SELECT
      ENCODE(ma.name, 'hex') AS asset_name,
      ma.fingerprint AS fingerprint,
      aic.total_supply::text,
      aic.decimals
    FROM multi_asset AS ma
    INNER JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
    WHERE ma.policy = _asset_policy_decoded;
END;
$$;

COMMENT ON FUNCTION grest.asset_policy_info IS 'Get a list of all asset under a policy'; -- noqa: LT01
