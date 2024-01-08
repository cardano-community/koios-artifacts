CREATE OR REPLACE FUNCTION grest.policy_asset_addresses(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  payment_address varchar,
  stake_address varchar,
  quantity text
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_policy_decoded bytea;
  _asset_ids int[];
  _isatoc int;
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  SELECT COUNT(ma_id) INTO _isatoc FROM grest.asset_tx_out_cache
    WHERE ma_id IN (SELECT id FROM multi_asset WHERE policy = _asset_policy_decoded);

  IF _isatoc > 0 THEN
    RETURN QUERY
      SELECT
        ENCODE(ma.name, 'hex') AS asset_name,
        x.address,
        x.stake_address,
        SUM(x.quantity)::text
      FROM 
        (
          SELECT
            atoc.ma_id,
            txo.address,
            sa.view AS stake_address,
            atoc.quantity
          FROM grest.asset_tx_out_cache AS atoc
          LEFT JOIN multi_asset AS ma ON ma.id = atoc.ma_id
          LEFT JOIN tx_out AS txo ON txo.id = atoc.txo_id
          LEFT JOIN stake_address AS sa ON txo.stake_address_id = sa.id
          WHERE ma.policy = DECODE(_asset_policy, 'hex')
            AND txo.consumed_by_tx_in_id IS NULL
        ) x
        LEFT JOIN multi_asset AS ma ON ma.id = x.ma_id
      GROUP BY
        ma.name,
        x.address;
  ELSE
    RETURN QUERY
      SELECT
        ENCODE(ma.name, 'hex') AS asset_name,
        txo.address,
        sa.view AS stake_address,
        SUM(mto.quantity)::text
      FROM multi_asset AS ma
      LEFT JOIN ma_tx_out AS mto ON mto.ident = ma.id
      LEFT JOIN tx_out AS txo ON txo.id = mto.tx_out_id
      LEFT JOIN stake_address AS sa ON txo.stake_address_id = sa.id
      WHERE ma.policy = DECODE(_asset_policy, 'hex')
        AND txo.consumed_by_tx_in_id IS NULL
      GROUP BY
        ma.name,
        txo.address;
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.policy_asset_addresses IS 'Returns a list of addresses with quantity for each asset ON a given policy'; -- noqa: LT01
