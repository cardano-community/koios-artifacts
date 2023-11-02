CREATE OR REPLACE FUNCTION grest.asset_info(_asset_list text [] [])
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
  token_registry_metadata jsonb,
  cip68_metadata jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_id_list bigint[];
BEGIN
  -- find all asset id's based ON nested array input
  SELECT INTO _asset_id_list ARRAY_AGG(id)
  FROM (
    SELECT DISTINCT mu.id
    FROM (
      SELECT
        DECODE(asset_list->>0, 'hex') AS policy,
        DECODE(asset_list->>1, 'hex') AS name
      FROM
      JSONB_ARRAY_ELEMENTS(TO_JSONB(_asset_list)) AS asset_list
    ) AS ald
    INNER JOIN multi_asset AS mu ON mu.policy = ald.policy AND mu.name = ald.name
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
      END,
      cip68.metadata
    FROM
      multi_asset AS ma
      INNER JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
      INNER JOIN tx ON tx.id = aic.last_mint_tx_id
      LEFT JOIN grest.asset_registry_cache AS arc ON arc.asset_policy = ENCODE(ma.policy,'hex') AND arc.asset_name = ENCODE(ma.name,'hex')
      LEFT JOIN LATERAL (
        SELECT
          JSONB_OBJECT_AGG(
            key::text,
            json
          ) AS minting_tx_metadata
        FROM
          tx_metadata AS tm
        WHERE
          tm.tx_id = tx.id
      ) metadata ON TRUE
      LEFT JOIN LATERAL (
        -- CIP-68 supported labels
        -- 100 = 000643b0 (ref, metadata)
        -- 222 = 000de140 (NFT)
        -- 333 = 0014df10 (FT)
        -- 444 = 001bc280 (RFT)
        SELECT
          CASE
            WHEN datum.value IS NULL THEN NULL
          ELSE
            JSONB_BUILD_OBJECT(
              CASE
                WHEN STARTS_WITH(ENCODE(ma.name, 'hex'), '000de140') THEN '222'
                WHEN STARTS_WITH(ENCODE(ma.name, 'hex'), '0014df10') THEN '333'
                WHEN STARTS_WITH(ENCODE(ma.name, 'hex'), '001bc280') THEN '444'
                ELSE '0'
              END,
              datum.value
            )
          END AS metadata
        FROM
          tx_out
          INNER JOIN datum ON datum.hash = tx_out.data_hash
        WHERE
          tx_out.id = (
            SELECT
              (SELECT MAX(tx_out_id) FROM ma_tx_out WHERE ident = _ma.id) as tx_id
            FROM
              multi_asset _ma
            WHERE
              _ma.policy = MA.policy
              AND
              _ma.name = (
                SELECT
                  CASE WHEN
                    STARTS_WITH(ENCODE(ma.name, 'hex'), '000de140')
                    OR
                    STARTS_WITH(ENCODE(ma.name, 'hex'), '0014df10')
                    OR
                    STARTS_WITH(ENCODE(ma.name, 'hex'), '001bc280')
                  THEN CONCAT('\x000643b0', SUBSTRING(ENCODE(ma.name, 'hex'), 9))::bytea
                  ELSE null
                  END
              )
          )
      ) cip68 ON TRUE
    WHERE
      ma.id = ANY(_asset_id_list);

END;
$$;
