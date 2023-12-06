CREATE OR REPLACE FUNCTION grest.account_assets(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
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
        sa.view,
        ma.policy,
        ma.name,
        ma.fingerprint,
        COALESCE(aic.decimals, 0) AS decimals,
        SUM(mtx.quantity) AS quantity
      FROM ma_tx_out AS mtx
      INNER JOIN multi_asset AS ma ON ma.id = mtx.ident
      LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
      INNER JOIN tx_out AS txo ON txo.id = mtx.tx_out_id
      INNER JOIN stake_address AS sa ON sa.id = txo.stake_address_id
      WHERE sa.view = ANY(_stake_addresses)
        AND txo.consumed_by_tx_in_id IS NULL
      GROUP BY
        sa.view, ma.policy, ma.name, ma.fingerprint, aic.decimals
    )

    SELECT
      aa.view AS stake_address,
      ENCODE(aa.policy, 'hex') AS policy_id,
      ENCODE(aa.name, 'hex') AS asset_name,
      aa.fingerprint AS fingerprint,
      aa.decimals AS decimals,
      aa.quantity::text AS quantity
    FROM _all_assets AS aa
    ORDER BY aa.view;
END;
$$;

COMMENT ON FUNCTION grest.account_assets IS 'Get the native asset balance of given accounts'; -- noqa: LT01
