CREATE OR REPLACE FUNCTION grest.policy_asset_addresses(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  payment_address varchar,
  quantity text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT
      ENCODE(ma.name, 'hex') AS asset_name,
      txo.address,
      SUM(mto.quantity)::text
    FROM multi_asset AS ma
    LEFT JOIN ma_tx_out AS mto ON mto.ident = ma.id
    LEFT JOIN tx_out AS txo ON txo.id = mto.tx_out_id
    WHERE ma.policy = DECODE(_asset_policy, 'hex')
      AND txo.consumed_by_tx_in_id IS NULL
    GROUP BY
      ma.name,
      txo.address;
END;
$$;

COMMENT ON FUNCTION grest.policy_asset_addresses IS 'Returns a list of addresses with quantity for each asset ON a given policy'; -- noqa: LT01
