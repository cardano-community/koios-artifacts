CREATE OR REPLACE FUNCTION grest.account_assets(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  asset_list jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  sa_id_list integer[];
BEGIN
  SELECT INTO sa_id_list
    ARRAY_AGG(stake_address.id)
  FROM
    stake_address
  WHERE
    stake_address.view = ANY(_stake_addresses);
  RETURN QUERY
    WITH _all_assets AS (
      SELECT
        sa.view,
        ma.policy,
        ma.name,
        ma.fingerprint,
        COALESCE(aic.decimals, 0) AS decimals,
        SUM(mtx.quantity) AS quantity
      FROM
        ma_tx_out AS mtx
        INNER JOIN multi_asset AS ma ON ma.id = mtx.ident
        INNER JOIN tx_out AS txo ON txo.id = mtx.tx_out_id
        INNER JOIN stake_address AS sa ON sa.id = txo.stake_address_id
        LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
      WHERE
        sa.id = ANY(sa_id_list)
        AND tx_out.consumed_by_tx_in_id IS NULL
      GROUP BY
        sa.view, ma.policy, ma.name, ma.fingerprint, aic.decimals
    )

    SELECT
      assets_grouped.view AS stake_address,
      assets_grouped.assets
    FROM (
      SELECT
        aa.view,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'policy_id', ENCODE(aa.policy, 'hex'),
            'asset_name', ENCODE(aa.name, 'hex'),
            'fingerprint', aa.fingerprint,
            'decimals', COALESCE(aa.decimals, 0),
            'quantity', aa.quantity::text
          )
        ) AS assets
      FROM
        _all_assets AS aa
      GROUP BY
        aa.view
    ) AS assets_grouped;
END;
$$;

COMMENT ON FUNCTION grest.account_assets IS 'Get the native asset balance of given accounts'; -- noqa: LT01