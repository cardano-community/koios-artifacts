CREATE OR REPLACE FUNCTION grest.cip67_label(_asset_name text)
RETURNS smallint
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  -- CIP-67 supported labels
  -- 100 = 000643b0 (ref, metadata)
  -- 222 = 000de140 (NFT)
  -- 333 = 0014df10 (FT)
  -- 444 = 001bc280 (RFT, rich-ft)
  -- 500 = 001f4d70 (NFT, royalty)

  IF (_asset_name like '000643b0%') THEN
    RETURN 100;
  ELSIF (_asset_name like '000de140%') THEN
    RETURN 222;
  ELSIF (_asset_name like '0014df10%') THEN
    RETURN 333;
  ELSIF (_asset_name like '001bc280%') THEN
    RETURN 444;
  ELSIF (_asset_name like '001f4d70%') THEN
    RETURN 500;
  ELSE
    RETURN 0;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION grest.cip67_strip_label(_asset_name text)
RETURNS text
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF (grest.cip67_label(_asset_name) != 0) THEN
    RETURN SUBSTRING(_asset_name FROM 9);
  ELSE
    RETURN _asset_name;
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.cip67_label IS 'Returns CIP-67 label for asset name or 0 if not a valid CIP-68 token'; -- noqa: LT01
COMMENT ON FUNCTION grest.cip67_strip_label IS 'Strips prefix from asset name matching CIP-67 standard'; -- noqa: LT01

