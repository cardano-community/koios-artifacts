CREATE OR REPLACE FUNCTION grest.asset_nft_address(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  payment_address varchar
) LANGUAGE plpgsql
AS $$
DECLARE
  _asset_policy_decoded bytea;
  _asset_name_decoded bytea;
  _asset_id int;
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  SELECT DECODE(
    CASE WHEN _asset_name IS NULL THEN ''
    ELSE _asset_name
    END,
    'hex'
  ) INTO _asset_name_decoded;

  SELECT id INTO _asset_id
  FROM multi_asset AS ma
  INNER JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
  WHERE ma.policy = _asset_policy_decoded
    AND ma.name = _asset_name_decoded
    AND aic.total_supply = 1;

  IF EXISTS (SELECT * FROM ma_tx_mint WHERE ident = _asset_id and quantity < 0 LIMIT 1) THEN
    RETURN QUERY
      SELECT address
      FROM tx_out
      WHERE id = (
        SELECT MAX(tx_out_id)
        FROM ma_tx_out
        WHERE ident = _asset_id
      );
  ELSE
    RETURN QUERY
      SELECT address
      FROM tx_out
      INNER JOIN ma_tx_out mto ON mto.tx_out_id = tx_out.id
      WHERE mto.ident = _asset_id
        AND tx_out.consumed_by_tx_in_id IS NULL
      ORDER BY tx_out.id DESC
      LIMIT 1;
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.asset_nft_address IS 'Returns the current address holding the specified NFT'; -- noqa: LT01
