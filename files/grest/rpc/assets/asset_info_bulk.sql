CREATE OR REPLACE FUNCTION grest.asset_info (_asset_list text[][])
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
    token_registry_metadata json
  )
  LANGUAGE PLPGSQL
  AS $$
DECLARE
  _asset_id_list      bigint[];
BEGIN

  -- find all asset id's based on nested array input
  SELECT INTO _asset_id_list ARRAY_AGG(id)
  FROM (
    SELECT DISTINCT mu.id
    FROM (
      SELECT
        DECODE(asset_list->>0, 'hex') AS policy,
        DECODE(asset_list->>1, 'hex') AS name
      FROM
      JSONB_ARRAY_ELEMENTS(TO_JSONB(_asset_list)) asset_list
    ) ald
    INNER JOIN multi_asset mu ON mu.policy = ald.policy AND mu.name = ald.name
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
      EXTRACT(epoch from aic.creation_time)::integer,
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
      END
    FROM
      multi_asset ma
      INNER JOIN grest.asset_info_cache aic ON aic.asset_id = ma.id
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
      ma.id = any (_asset_id_list);

END;
$$;
