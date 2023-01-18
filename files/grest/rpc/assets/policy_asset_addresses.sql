CREATE OR REPLACE FUNCTION grest.policy_asset_addresses (_asset_policy text)
  RETURNS TABLE (
    asset_name text,
    payment_address varchar,
    quantity text
  ) LANGUAGE PLPGSQL
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
        FROM
          multi_asset ma
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
        FROM
          _all_assets aa
          INNER JOIN ma_tx_out mto ON mto.ident = aa.id
          INNER JOIN tx_out txo ON txo.id = mto.tx_out_id
          LEFT JOIN tx_in ON txo.tx_id = tx_in.tx_out_id
            AND txo.index::smallint = tx_in.tx_out_index::smallint
        WHERE
          tx_in.id IS NULL
      ) x
    GROUP BY
      x.asset_name, x.address;
END;
$$;

COMMENT ON FUNCTION grest.policy_asset_addresses IS 'Returns a list of addresses with quantity for each asset on a given policy';
