CREATE OR REPLACE FUNCTION grest.asset_info(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  policy_id text,
  asset_name text,
  asset_name_ascii text,
  fingerprint character varying,
  minting_tx_hash text,
  total_supply text,
  mint_cnt bigint,
  burn_cnt bigint,
  creation_time integer,
  minting_tx_metadata jsonb,
  token_registry_metadata jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_id_list      bigint[];
BEGIN
  -- find all asset id's based ON nested array input
  SELECT INTO _asset_id_list ARRAY_AGG(id)
  FROM (
    SELECT DISTINCT mu.id
    FROM multi_asset AS mu
    WHERE mu.policy = DECODE(_asset_policy, 'hex')
      AND mu.name = DECODE(_asset_name, 'hex')
  ) AS tmp;
  RETURN QUERY
    SELECT
      ENCODE(ma.policy, 'hex'),
      ENCODE(ma.name, 'hex'),
      ENCODE(ma.name, 'escape'),
      ma.fingerprint,
      ENCODE(tx.hash, 'hex'),
      aic.total_supply::text,
      aic.mint_cnt,
      aic.burn_cnt,
      EXTRACT(EPOCH FROM aic.creation_time)::integer,
      metadata.minting_tx_metadata,
      CASE WHEN arc.name IS NULL THEN NULL
      ELSE
        JSONB_BUILD_OBJECT(
          'name', arc.name,
          'description', arc.description,
          'ticker', arc.ticker,
          'url', arc.url,
          'logo', arc.logo,
          'decimals', arc.decimals
        )
      END
    FROM multi_asset AS ma
      INNER JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
      INNER JOIN tx ON tx.id = aic.last_mint_tx_id
      LEFT JOIN grest.asset_registry_cache AS arc ON arc.asset_policy = ENCODE(ma.policy,'hex') AND arc.asset_name = ENCODE(ma.name,'hex')
      LEFT JOIN LATERAL (
        SELECT JSONB_OBJECT_AGG(key::text, json ) AS minting_tx_metadata
        FROM tx_metadata AS tm
        WHERE tm.tx_id = tx.id
      ) metadata ON TRUE
    WHERE ma.id = ANY(_asset_id_list);

END;
$$;
