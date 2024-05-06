CREATE TABLE IF NOT EXISTS grest.epoch_info_cache (
  epoch_no word31type PRIMARY KEY NOT NULL,
  i_out_sum word128type,
  i_fees lovelace,
  i_tx_count word31type,
  i_blk_count word31type,
  i_first_block_time numeric UNIQUE,
  i_last_block_time numeric UNIQUE,
  i_total_rewards lovelace,
  i_avg_blk_reward lovelace,
  i_last_tx_id bigint,
  p_nonce text,
  p_block_hash text
);

COMMENT ON TABLE grest.epoch_info_cache IS 'Contains detailed info for epochs including protocol parameters';

CREATE OR REPLACE FUNCTION grest.epoch_info_cache_update(
  _epoch_no_to_insert_from bigint DEFAULT NULL
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
    SELECT COUNT(pid) > 1
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query ILIKE '%grest.epoch_info_cache_update%'
      AND datname = (SELECT current_database())
    ) THEN
        RAISE EXCEPTION 'Previous epoch_info_cache_update query still running but should have completed! Exiting...';
  END IF;

  SELECT MAX(no) INTO _curr_epoch
  FROM public.epoch;

  IF _epoch_no_to_insert_from IS NULL THEN
    SELECT COALESCE(MAX(epoch_no), 0) INTO _latest_epoch_no_in_cache
    FROM grest.epoch_info_cache
    WHERE i_first_block_time IS NOT NULL;

    IF _latest_epoch_no_in_cache = 0 THEN
      RAISE NOTICE 'Epoch info cache table is empty, starting initial population...';
      PERFORM grest.epoch_summary_corrections_update();
      PERFORM grest.epoch_info_cache_update(0);
      RETURN;
    END IF;

    RAISE NOTICE 'Latest epoch in cache: %, current epoch: %.', _latest_epoch_no_in_cache, _curr_epoch;

    IF _curr_epoch = _latest_epoch_no_in_cache THEN
      RAISE NOTICE 'Updating latest epoch info in cache...';
      PERFORM grest.update_latest_epoch_info_cache(_curr_epoch, _latest_epoch_no_in_cache);
      RETURN;
    END IF;

    IF _latest_epoch_no_in_cache > _curr_epoch THEN
      RAISE NOTICE 'No update needed, exiting...';
      RETURN;
    END IF;

    RAISE NOTICE 'Updating cache with new epoch(s) data...';
    -- We need to update last epoch one last time before going to new one
    PERFORM grest.update_latest_epoch_info_cache(_curr_epoch, _latest_epoch_no_in_cache);
    -- Populate rewards data for epoch n - 2
    PERFORM grest.update_total_rewards_epoch_info_cache(_latest_epoch_no_in_cache - 1);
    -- Continue new epoch data insert
    _epoch_no_to_insert_from := _latest_epoch_no_in_cache + 1;
  END IF;

  RAISE NOTICE 'Deleting cache records FROM epoch % onwards...', _epoch_no_to_insert_from;
  DELETE FROM grest.epoch_info_cache
  WHERE epoch_no >= _epoch_no_to_insert_from;

  DROP TABLE IF EXISTS last_tx_id_subset;
  CREATE TEMP TABLE last_tx_id_subset (
    epoch_no bigint,
    tx_id bigint
  );

  INSERT INTO last_tx_id_subset
    SELECT b.epoch_no, MAX(tx.id)
    FROM block AS b
    INNER JOIN tx ON tx.block_id = b.id
    WHERE b.block_no IS NOT NULL
      AND b.tx_count != 0
    GROUP BY b.epoch_no;

  INSERT INTO grest.epoch_info_cache
    SELECT DISTINCT ON (b.time)
      e.no AS epoch_no,
      e.out_sum AS i_out_sum,
      e.fees AS i_fees,
      e.tx_count AS i_tx_count,
      e.blk_count AS i_blk_count,
      EXTRACT(EPOCH FROM e.start_time) AS i_first_block_time,
      EXTRACT(EPOCH FROM e.end_time) AS i_last_block_time,
      CASE -- populated in epoch n + 2
        WHEN e.no <= _curr_epoch - 2 THEN reward_pot.amount
        ELSE NULL
      END AS i_total_rewards,
      CASE -- populated in epoch n + 2
        WHEN e.no <= _curr_epoch THEN ROUND(reward_pot.amount / e.blk_count)
        ELSE NULL
      END AS i_avg_blk_reward,
      (
        SELECT MAX(tx_id)
        FROM last_tx_id_subset
        WHERE epoch_no <= e.no
      ) AS i_last_tx_id,
      ENCODE(ep.nonce, 'hex') AS p_nonce,
      ENCODE(b.hash, 'hex') AS p_block_hash
    FROM epoch AS e
    LEFT JOIN epoch_param AS ep ON ep.epoch_no = e.no
    LEFT JOIN cost_model AS cm ON cm.id = ep.cost_model_id
    INNER JOIN block AS b ON b.time = e.start_time
    LEFT JOIN LATERAL (
        SELECT
          e.no,
          SUM(r.amount) AS amount
        FROM reward AS r
        WHERE r.earned_epoch = e.no
        GROUP BY
          e.no
      ) AS reward_pot ON TRUE
    WHERE e.no >= _epoch_no_to_insert_from
    ORDER BY
      b.time ASC,
      b.id ASC,
      e.no ASC;

  -- GREST control table entry
  PERFORM grest.update_control_table(
    'epoch_info_cache_last_updated',
    (now() at time zone 'utc')::text
  );

END;
$$;

-- Helper function for updating current epoch data
CREATE OR REPLACE FUNCTION grest.update_latest_epoch_info_cache(_curr_epoch bigint, _epoch_no_to_update bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- only update last tx id in case of new epoch
  IF _curr_epoch <> _epoch_no_to_update THEN
    UPDATE grest.epoch_info_cache
    SET i_last_tx_id = last_tx.tx_id
    FROM (
      SELECT MAX(tx.id) AS tx_id
      FROM block AS b
      INNER JOIN tx ON tx.block_id = b.id
      WHERE b.epoch_no <= _epoch_no_to_update
        AND b.block_no IS NOT NULL
        AND b.tx_count != 0
    ) AS last_tx
    WHERE epoch_no = _epoch_no_to_update;
  END IF;

  UPDATE grest.epoch_info_cache
  SET
    i_out_sum = update_table.out_sum,
    i_fees = update_table.fees,
    i_tx_count = update_table.tx_count,
    i_blk_count = update_table.blk_count,
    i_last_block_time = EXTRACT(EPOCH FROM update_table.end_time)
  FROM (
    SELECT
      e.out_sum,
      e.fees,
      e.tx_count,
      e.blk_count,
      e.end_time
    FROM epoch AS e
    WHERE e.no = _epoch_no_to_update
  ) AS update_table
  WHERE epoch_no = _epoch_no_to_update;
END;
$$;

-- Helper function for updating epoch total rewards (epoch n - 2)
CREATE OR REPLACE FUNCTION grest.update_total_rewards_epoch_info_cache(_epoch_no_to_update bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE grest.epoch_info_cache
  SET
    i_total_rewards = update_t.amount,
    i_avg_blk_reward = update_t.avg_blk_reward
  FROM (
    SELECT
      reward_pot.amount,
      ROUND(reward_pot.amount /  e.blk_count) AS avg_blk_reward
    FROM (
      SELECT
        r.earned_epoch,
        SUM(r.amount) AS amount
      FROM reward AS r
      WHERE r.earned_epoch = _epoch_no_to_update
      GROUP BY r.earned_epoch
    ) AS reward_pot
    INNER JOIN epoch AS e ON reward_pot.earned_epoch = e.no
      AND e.no = _epoch_no_to_update
  ) AS update_t
  WHERE epoch_no = _epoch_no_to_update;
END;
$$;
