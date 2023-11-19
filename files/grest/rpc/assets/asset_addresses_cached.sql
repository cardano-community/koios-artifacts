CREATE OR REPLACE FUNCTION grest.asset_addresses_cached(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  payment_address varchar,
  quantity text
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_policy_decoded bytea;
  _asset_name_decoded bytea;
  _asset_id int;
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  SELECT DECODE(CASE
    WHEN _asset_name IS NULL THEN ''
    ELSE _asset_name
    END, 'hex') INTO _asset_name_decoded;
  SELECT id INTO _asset_id
  FROM multi_asset AS ma
  WHERE ma.policy = _asset_policy_decoded
    AND ma.name = _asset_name_decoded;

  RETURN QUERY
    SELECT
      x.address,
      SUM(x.quantity)::text
    FROM
      (
        SELECT
          txo.address,
          atoc.quantity
        FROM grest.asset_tx_out_cache AS atoc
        LEFT JOIN tx_out AS txo ON atoc.txo_id = txo.id
        WHERE atoc.ma_id = _asset_id
      ) AS x
    GROUP BY x.address;
END;
$$;

COMMENT ON FUNCTION grest.asset_addresses_cached IS 'Returns a list of addresses with quantity holding the specified asset (only valid for whitelisted cached assets)'; -- noqa: LT01
