CREATE OR REPLACE FUNCTION grest.policy_asset_addresses(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  payment_address varchar,
  quantity text
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_policy_decoded bytea;
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  RETURN QUERY
    WITH
      _all_assets AS (
        SELECT
          id,
          ENCODE(name, 'hex') AS asset_name
        FROM multi_asset AS ma
        WHERE ma.policy = _asset_policy_decoded
      )

    SELECT
      x.asset_name,
      x.address,
      SUM(x.quantity)::text
    FROM
      (
        SELECT
          aa.asset_name,
          txo.address,
          mto.quantity
        FROM _all_assets AS aa
        INNER JOIN ma_tx_out AS mto ON mto.ident = aa.id
        INNER JOIN tx_out AS txo ON txo.id = mto.tx_out_id
        WHERE tx_out.consumed_by_tx_in_id IS NULL
      ) AS x
    GROUP BY
      x.asset_name,
      x.address;
END;
$$;

COMMENT ON FUNCTION grest.policy_asset_addresses IS 'Returns a list of addresses with quantity for each asset ON a given policy'; -- noqa: LT01
