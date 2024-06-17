CREATE TABLE IF NOT EXISTS grest.asset_info_cache (
  asset_id bigint PRIMARY KEY NOT NULL,
  creation_time date,
  total_supply numeric,
  decimals integer,
  mint_cnt bigint,
  burn_cnt bigint,
  last_mint_tx_id bigint,
  last_mint_meta_tx_id bigint
);

CREATE INDEX IF NOT EXISTS idx_last_mint_tx_id ON grest.asset_info_cache (last_mint_tx_id);
CREATE INDEX IF NOT EXISTS idx_last_mint_meta_tx_id ON grest.asset_info_cache (last_mint_meta_tx_id);
CREATE INDEX IF NOT EXISTS idx_creation_time ON grest.asset_info_cache (creation_time DESC);

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

  -- assumption rollback to cater for - 15 blocks (16 tx each) , accordingly - rounding off to 250
  SELECT COALESCE(last_value::bigint,250) - 250 INTO _asset_info_cache_last_tx_id
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
        MAX(mtm.tx_id) AS last_mint_tx_id,
        MAX(mtm.tx_id) FILTER(WHERE tm.tx_id IS NOT NULL) AS last_mint_meta_tx_id
      FROM ma_tx_mint AS mtm
      LEFT JOIN tx_metadata AS tm ON tm.tx_id = mtm.tx_id
      WHERE
        CASE
          WHEN _asset_info_cache_last_tx_id IS NOT NULL AND _asset_id_list IS NOT NULL THEN
            mtm.ident = ANY(_asset_id_list)
            AND mtm.tx_id > _asset_info_cache_last_tx_id
          ELSE TRUE
        END
        AND mtm.quantity > 0
      GROUP BY mtm.ident
    )

  INSERT INTO grest.asset_info_cache
    SELECT
      ma.id,
      MIN(b.time) AS creation_time,
      SUM(mtm.quantity) AS total_supply,
      COALESCE(arc.decimals, 0) AS decimals,
      SUM(CASE WHEN mtm.quantity > 0 THEN 1 ELSE 0 END) AS mint_cnt,
      SUM(CASE WHEN mtm.quantity < 0 THEN 1 ELSE 0 END) AS burn_cnt,
      tm.last_mint_tx_id AS last_mint_tx_id,
      tm.last_mint_meta_tx_id AS last_mint_meta_tx_id
    FROM multi_asset AS ma
      INNER JOIN ma_tx_mint AS mtm ON mtm.ident = ma.id
      INNER JOIN tx ON tx.id = mtm.tx_id
      INNER JOIN block AS b ON b.id = tx.block_id
      INNER JOIN tx_mint_meta AS tm ON tm.ident = ma.id
      LEFT JOIN grest.asset_registry_cache AS arc ON arc.asset_policy = ENCODE(ma.policy,'hex') AND arc.asset_name = encode(ma.name,'hex')
    WHERE
      CASE
        WHEN _asset_info_cache_last_tx_id IS NOT NULL AND _asset_id_list IS NOT NULL THEN
          mtm.ident = ANY(_asset_id_list)
        ELSE TRUE
      END
    GROUP BY ma.id, arc.decimals, tm.last_mint_tx_id, tm.last_mint_meta_tx_id
  ON CONFLICT (asset_id)
  DO UPDATE SET
    creation_time = excluded.creation_time,
    total_supply = excluded.total_supply,
    decimals = excluded.decimals,
    mint_cnt = excluded.mint_cnt,
    burn_cnt = excluded.burn_cnt,
    last_mint_tx_id = excluded.last_mint_tx_id,
    last_mint_meta_tx_id = COALESCE(excluded.last_mint_meta_tx_id,grest.asset_info_cache.last_mint_meta_tx_id);

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
