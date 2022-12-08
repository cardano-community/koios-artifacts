CREATE FUNCTION grest.asset_policy_info (_asset_policy text)
  RETURNS TABLE (
    asset_name text,
    asset_name_ascii text,
    fingerprint varchar,
    minting_tx_metadata jsonb,
    token_registry_metadata jsonb,
    total_supply text,
    creation_time integer
  )
  LANGUAGE PLPGSQL
  AS $$
DECLARE
  _asset_policy_decoded bytea;
  _policy_asset_ids bigint[];
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  
  RETURN QUERY 
    SELECT
      ENCODE(ma.name, 'hex') AS asset_name,
      ENCODE(ma.name, 'escape') AS asset_name_ascii,
      ma.fingerprint,
      metadata.minting_tx_metadata,
      CASE WHEN arc.name IS NULL THEN NULL
      ELSE
        JSON_BUILD_OBJECT(
          'name', arc.name,
          'description', arc.description,
          'ticker', arc.ticker,
          'url', arc.url,
          'logo', arc.logo,
          'decimals', arc.decimals
        )
      END,
      aic.total_supply,
      EXTRACT(epoch from aic.creation_time)::integer
    FROM 
      multi_asset ma
      LEFT JOIN grest.asset_info_cache aic ON aic.asset_id = ma.id
      LEFT JOIN tx ON tx.id = aic.last_mint_tx_id
      LEFT JOIN grest.asset_registry_cache arc ON DECODE(arc.asset_policy, 'hex') = ma.policy AND DECODE(arc.asset_name, 'hex') = ma.name
      LEFT JOIN LATERAL (
        SELECT
          JSONB_OBJECT_AGG(
            key::text,
            json
          ) AS minting_tx_metadata
        FROM
          tx_metadata tm
        WHERE
          tm.tx_id = tx.id
      ) metadata ON TRUE
    WHERE
      ma.policy = _asset_policy_decoded;
END;
$$;

COMMENT ON FUNCTION grest.asset_policy_info IS 'Get the asset information of all assets under a policy';

