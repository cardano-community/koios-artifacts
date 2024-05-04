CREATE OR REPLACE FUNCTION grest.address_assets(_addresses text [])
RETURNS TABLE (
  address varchar,
  policy_id text,
  asset_name text,
  fingerprint varchar,
  decimals integer,
  quantity text
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
        AND txo.consumed_by_tx_id IS NULL
      GROUP BY
        txo.address, ma.policy, ma.name, ma.fingerprint, aic.decimals
    )

    SELECT
      aa.address,
      ENCODE(aa.policy, 'hex') AS policy_id,
      ENCODE(aa.name, 'hex') AS asset_name,
      aa.fingerprint AS fingerprint,
      aa.decimals AS decimals,
      aa.quantity::text AS quantity
    FROM _all_assets AS aa
    ORDER BY aa.address;
END;
$$;

COMMENT ON FUNCTION grest.address_assets IS 'Get the list of all the assets (policy, name and quantity) for given addresses'; --noqa: LT01
