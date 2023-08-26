CREATE OR REPLACE FUNCTION grest.asset_address_list(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  payment_address varchar,
  quantity text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_addresses(_asset_policy, _asset_name);
END;
$$;

CREATE OR REPLACE FUNCTION grest.asset_addresses(_asset_policy text, _asset_name text DEFAULT '')
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
          mto.quantity
        FROM ma_tx_out AS mto
        INNER JOIN tx_out AS txo ON txo.id = mto.tx_out_id
        WHERE mto.ident = _asset_id
          AND tx_out.consumed_by_tx_in_id IS NULL
      ) AS x
    GROUP BY x.address;
END;
$$;

COMMENT ON FUNCTION grest.asset_address_list IS 'DEPRECATED!! Use asset_addresses instead.'; -- noqa: LT01
COMMENT ON FUNCTION grest.asset_addresses IS 'Returns a list of addresses with quantity holding the specified asset'; -- noqa: LT01
