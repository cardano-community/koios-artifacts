CREATE OR REPLACE FUNCTION grest.address_assets(_addresses text [])
RETURNS TABLE (
  address varchar,
  asset_list jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY

  WITH _all_assets AS (
    SELECT
      txo.address,
      ma.policy,
      ma.name,
      ma.fingerprint,
      COALESCE(aic.decimals, 0) AS decimals,
      SUM(mtx.quantity) AS quantity
    FROM ma_tx_out AS mtx
    INNER JOIN multi_asset AS ma ON ma.id = mtx.ident
    LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
    INNER JOIN tx_out AS txo ON txo.id = mtx.tx_out_id
    WHERE txo.address = ANY(_addresses)
      AND tx_out.consumed_by_tx_in_id IS NULL
    GROUP BY
      txo.address, ma.policy, ma.name, ma.fingerprint, aic.decimals
  )

  SELECT
    assets_grouped.address,
    assets_grouped.asset_list
  FROM (
    SELECT
      aa.address,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'policy_id', ENCODE(aa.policy, 'hex'),
          'asset_name', ENCODE(aa.name, 'hex'),
          'fingerprint', aa.fingerprint,
          'decimals', aa.decimals,
          'quantity', aa.quantity::text
        )
      ) AS asset_list
    FROM _all_assets AS aa
    GROUP BY aa.address
  ) assets_grouped;
END;
$$;

COMMENT ON FUNCTION grest.address_assets IS 'Get the list of all the assets (policy, name and quantity) for given addresses'; --noqa: LT01
