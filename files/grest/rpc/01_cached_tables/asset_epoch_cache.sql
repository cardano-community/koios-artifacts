CREATE TABLE IF NOT EXISTS grest.asset_epoch_cache (
  epoch_no word31type NOT NULL,
  asset_id bigint NOT NULL,
  minted numeric,
  mint_cnt bigint,
  burned numeric,
  burn_cnt bigint,
  PRIMARY KEY (epoch_no, asset_id)
);

CREATE OR REPLACE FUNCTION grest.asset_epoch_cache_update (
    _epoch_no_to_insert_from bigint default NULL
  )
  RETURNS void
  LANGUAGE plpgsql
  AS $$
DECLARE
  _curr_epoch bigint;
  _latest_epoch_no_in_cache bigint;
BEGIN
  -- Check previous cache update completed before running
  IF (
    SELECT
      COUNT(pid) > 1
    FROM
      pg_stat_activity
    WHERE
      state = 'active' AND query ILIKE '%grest.asset_epoch_cache_update%'
      AND datname = (SELECT current_database())
  ) THEN 
    RAISE EXCEPTION 'Previous asset_epoch_cache_update query still running but should have completed! Exiting...';
  END IF;

  -- GREST control table entry
  PERFORM grest.update_control_table(
    'asset_epoch_cache_last_updated',
    (now() at time zone 'utc')::text
  );

  SELECT
    MAX(no) INTO _curr_epoch
  FROM
    public.epoch;

  IF _epoch_no_to_insert_from IS NULL THEN

    SELECT
      COALESCE(MAX(epoch_no), 0) INTO _latest_epoch_no_in_cache
    FROM
      grest.asset_epoch_cache;

    IF _latest_epoch_no_in_cache = 0 THEN
      RAISE NOTICE 'Asset epoch cache table is empty, starting initial population...';
      PERFORM grest.asset_epoch_cache_update(0);
      RETURN;
    END IF;

    RAISE NOTICE 'Latest epoch in cache: %, current epoch: %.', _latest_epoch_no_in_cache, _curr_epoch;

    IF _curr_epoch = _latest_epoch_no_in_cache THEN
      RAISE NOTICE 'Updating assets for latest epoch in cache...';
      PERFORM grest.update_latest_asset_epoch_cache(_latest_epoch_no_in_cache);
      RETURN;
    END IF;

    RAISE NOTICE 'Updating cache with new epoch(s) data...';
    -- We need to update last epoch one last time before going to new one
    PERFORM grest.update_latest_asset_epoch_cache(_latest_epoch_no_in_cache);
    -- Continue new epoch data insert
    _epoch_no_to_insert_from := _latest_epoch_no_in_cache + 1;

  END IF;

  RAISE NOTICE 'Deleting cache records from epoch % onwards...', _epoch_no_to_insert_from;
  DELETE FROM grest.asset_epoch_cache
    WHERE epoch_no >= _epoch_no_to_insert_from;

  INSERT INTO grest.asset_epoch_cache
    SELECT
      b.epoch_no,
      mtm.ident AS asset_id,
      SUM(CASE WHEN mtm.quantity > 0 THEN mtm.quantity ELSE 0 END) AS minted,
      SUM(CASE WHEN mtm.quantity > 0 THEN 1 ELSE 0 END) AS mint_cnt,
      SUM(CASE WHEN mtm.quantity < 0 THEN ABS(mtm.quantity) ELSE 0 END) AS burned,
      SUM(CASE WHEN mtm.quantity < 0 THEN 1 ELSE 0 END) AS burn_cnt
    FROM
      ma_tx_mint mtm
      INNER JOIN tx ON tx.id = mtm.tx_id
      INNER JOIN block b ON b.id = tx.block_id
    WHERE
      b.epoch_no >= _epoch_no_to_insert_from
    GROUP BY
      epoch_no, asset_id;
END;
$$;

-- Helper function for updating current epoch data
CREATE OR REPLACE FUNCTION grest.update_latest_asset_epoch_cache (_epoch_no_to_update bigint)
  RETURNS void
  LANGUAGE plpgsql
  AS $$
BEGIN
  INSERT INTO grest.asset_epoch_cache
    SELECT
      b.epoch_no,
      mtm.ident AS asset_id,
      SUM(CASE WHEN mtm.quantity > 0 THEN mtm.quantity ELSE 0 END) AS minted,
      SUM(CASE WHEN mtm.quantity > 0 THEN 1 ELSE 0 END) AS mint_cnt,
      SUM(CASE WHEN mtm.quantity < 0 THEN ABS(mtm.quantity) ELSE 0 END) AS burned,
      SUM(CASE WHEN mtm.quantity < 0 THEN 1 ELSE 0 END) AS burn_cnt
    FROM
      ma_tx_mint mtm
      INNER JOIN tx ON tx.id = mtm.tx_id
      INNER JOIN block b ON b.id = tx.block_id
    WHERE
      b.epoch_no = _epoch_no_to_update
    GROUP BY
      epoch_no, asset_id
  ON CONFLICT (epoch_no, asset_id)
  DO UPDATE SET
    minted = excluded.minted,
    burned = excluded.burned;
END;
$$;
