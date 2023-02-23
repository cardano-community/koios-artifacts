CREATE OR REPLACE FUNCTION grest.asset_address_list (_asset_policy text, _asset_name text default '')
  RETURNS TABLE (
    payment_address varchar,
    quantity text
  ) LANGUAGE PLPGSQL
  AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_addresses(_asset_policy, _asset_name);
END;
$$;

CREATE OR REPLACE FUNCTION grest.asset_addresses (_asset_policy text, _asset_name text default '')
  RETURNS TABLE (
    payment_address varchar,
    quantity text
  ) LANGUAGE PLPGSQL
  AS $$
DECLARE
  _asset_policy_decoded bytea;
  _asset_name_decoded bytea;
  _asset_id int;
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  SELECT DECODE(
    CASE WHEN _asset_name IS NULL
      THEN ''
    ELSE
      _asset_name
    END,
    'hex'
  ) INTO _asset_name_decoded;
  SELECT id INTO _asset_id FROM multi_asset ma WHERE ma.policy = _asset_policy_decoded AND ma.name = _asset_name_decoded;

  RETURN QUERY
    SELECT
      x.address,
      SUM(x.quantity)::text
    FROM
      (
        SELECT
          txo.address,
          mto.quantity
        FROM
          ma_tx_out mto
          INNER JOIN tx_out txo ON txo.id = mto.tx_out_id
          LEFT JOIN tx_in ON txo.tx_id = tx_in.tx_out_id
            AND txo.index::smallint = tx_in.tx_out_index::smallint
        WHERE
          mto.ident = _asset_id
          AND tx_in.tx_out_id IS NULL
      ) x
    GROUP BY
      x.address;
END;
$$;

COMMENT ON FUNCTION grest.asset_address_list IS 'DEPRECATED!! Use asset_addresses instead.';
COMMENT ON FUNCTION grest.asset_addresses IS 'Returns a list of addresses with quantity holding the specified asset';
