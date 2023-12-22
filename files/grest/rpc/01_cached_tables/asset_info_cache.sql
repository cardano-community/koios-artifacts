CREATE TABLE IF NOT EXISTS grest.asset_info_cache (
  asset_id bigint PRIMARY KEY NOT NULL,
  creation_time date,
  total_supply numeric,
  decimals integer,
  mint_cnt bigint,
  burn_cnt bigint,
  first_mint_tx_id bigint,
  first_mint_keys text [],
  last_mint_tx_id bigint,
  last_mint_keys text []
);

CREATE INDEX IF NOT EXISTS idx_first_mint_tx_id ON grest.asset_info_cache (first_mint_tx_id);
CREATE INDEX IF NOT EXISTS idx_last_mint_tx_id ON grest.asset_info_cache (last_mint_tx_id);

CREATE OR REPLACE FUNCTION grest.asset_info_cache_update()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  _lastest_tx_id bigint;
  _asset_info_cache_last_tx_id bigint;
  _asset_id_list bigint[];
BEGIN
  -- Check previous cache update completed before running
  IF (
    SELECT COUNT(pid) > 1
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query ILIKE '%grest.asset_info_cache_update%'
      AND datname = (SELECT current_database())
  ) THEN
    RAISE EXCEPTION 'Previous asset_info_cache_update query still running but should have completed! Exiting...';
  END IF;

  SELECT MAX(id) INTO _lastest_tx_id
  FROM public.tx;

  SELECT COALESCE(last_value::bigint,1000) - 1000 INTO _asset_info_cache_last_tx_id
  FROM grest.control_table
  WHERE key = 'asset_info_cache_last_tx_id';

  IF _asset_info_cache_last_tx_id IS NULL THEN
    RAISE NOTICE 'Asset info cache table is empty, deleting all previous cache records and starting initial population...';
    TRUNCATE TABLE grest.asset_info_cache;
  ELSE
    RAISE NOTICE 'Updating asset info based ON data FROM transaction id in range % - % ...', _asset_info_cache_last_tx_id, _lastest_tx_id;
    SELECT ARRAY_AGG(DISTINCT ident) INTO _asset_id_list
    FROM ma_tx_mint
    WHERE tx_id > _asset_info_cache_last_tx_id;
  END IF;

  WITH

    tx_mint_meta AS (
      SELECT
        mtm.ident,
        MIN(mtm.tx_id) AS first_mint_tx_id,
        MAX(mtm.tx_id) AS last_mint_tx_id
      FROM ma_tx_mint AS mtm
      INNER JOIN tx_metadata AS tm ON tm.tx_id = mtm.tx_id
      WHERE
        CASE WHEN _asset_info_cache_last_tx_id IS NOT NULL AND _asset_id_list IS NOT NULL
          THEN
            mtm.ident = ANY(_asset_id_list)
            AND mtm.tx_id > _asset_info_cache_last_tx_id
          ELSE TRUE
        END
        AND tm.json IS NOT NULL
        AND mtm.quantity > 0
      GROUP BY mtm.ident
    ),

    tx_mint_nometa AS (
      SELECT
        mtm.ident,
        MIN(mtm.tx_id) AS first_mint_tx_id,
        MAX(mtm.tx_id) AS last_mint_tx_id
      FROM ma_tx_mint AS mtm
      LEFT JOIN tx_mint_meta ON tx_mint_meta.ident = mtm.ident
      WHERE
        CASE WHEN _asset_info_cache_last_tx_id IS NOT NULL AND _asset_id_list IS NOT NULL
          THEN
            mtm.ident = ANY(_asset_id_list)
            AND mtm.tx_id > _asset_info_cache_last_tx_id
          ELSE TRUE
        END
        AND tx_mint_meta IS NULL
      GROUP BY mtm.ident
    ),

    tx_meta AS (
      SELECT
        tmm.ident,
        tmm.first_mint_tx_id,
        ARRAY_AGG(tm.key) FILTER(WHERE tm.tx_id = tmm.first_mint_tx_id) AS first_mint_keys,
        tmm.last_mint_tx_id,
        ARRAY_AGG(tm.key) FILTER(WHERE tm.tx_id = tmm.last_mint_tx_id) AS last_mint_keys
      FROM tx_mint_meta AS tmm
      INNER JOIN tx_metadata AS tm ON tm.tx_id = tmm.first_mint_tx_id OR tm.tx_id = tmm.last_mint_tx_id
      GROUP BY tmm.ident, tmm.first_mint_tx_id, tmm.last_mint_tx_id
      --
      UNION ALL
      --
      SELECT
        tx_mint_nometa.ident,
        tx_mint_nometa.first_mint_tx_id,
        '{}',
        tx_mint_nometa.last_mint_tx_id,
        '{}'
      FROM tx_mint_nometa
    )

  INSERT INTO grest.asset_info_cache
    SELECT
      ma.id,
      MIN(B.time) AS creation_time,
      SUM(mtm.quantity) AS total_supply,
      COALESCE(arc.decimals, 0) AS decimals,
      SUM(CASE WHEN mtm.quantity > 0 THEN 1 ELSE 0 END) AS mint_cnt,
      SUM(CASE WHEN mtm.quantity < 0 THEN 1 ELSE 0 END) AS burn_cnt,
      tm.first_mint_tx_id,
      tm.first_mint_keys,
      tm.last_mint_tx_id,
      tm.last_mint_keys
    FROM
      multi_asset AS ma
      INNER JOIN ma_tx_mint AS mtm ON mtm.ident = ma.id
      INNER JOIN tx ON tx.id = mtm.tx_id
      INNER JOIN block AS b ON b.id = tx.block_id
      INNER JOIN tx_meta AS tm ON tm.ident = ma.id
      LEFT JOIN grest.asset_registry_cache AS arc ON arc.asset_policy = ENCODE(ma.policy,'hex') AND arc.asset_name = encode(ma.name,'hex')
    WHERE
      CASE WHEN _asset_info_cache_last_tx_id IS NOT NULL AND _asset_id_list IS NOT NULL
        THEN
          mtm.ident = ANY(_asset_id_list)
        ELSE TRUE
      END
    GROUP BY ma.id, arc.decimals, tm.first_mint_tx_id, tm.first_mint_keys, tm.last_mint_tx_id, tm.last_mint_keys
  ON CONFLICT (asset_id)
  DO UPDATE SET
    creation_time = excluded.creation_time,
    total_supply = excluded.total_supply,
    decimals = excluded.decimals,
    mint_cnt = excluded.mint_cnt,
    burn_cnt = excluded.burn_cnt,
    last_mint_tx_id = excluded.last_mint_tx_id,
    last_mint_keys = excluded.last_mint_keys;

  IF _asset_info_cache_last_tx_id IS NOT NULL AND _asset_id_list IS NOT NULL THEN
    RAISE NOTICE '% assets added or updated', ARRAY_LENGTH(_asset_id_list, 1);
  END IF;

  -- GREST control table entry
  PERFORM grest.update_control_table(
    'asset_info_cache_last_tx_id',
    _lastest_tx_id::text
  );

END;
$$;
