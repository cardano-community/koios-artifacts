DROP TABLE IF EXISTS grest.pool_history_cache;

CREATE TABLE grest.pool_history_cache (
  pool_id varchar,
  epoch_no int8 NULL,
  active_stake lovelace NULL,
  active_stake_pct numeric NULL,
  saturation_pct numeric NULL,
  block_cnt int8 NULL,
  delegator_cnt int8 NULL,
  pool_fee_variable float8 NULL,
  pool_fee_fixed lovelace NULL,
  pool_fees float8 NULL,
  deleg_rewards float8 NULL,
  member_rewards float8 NULL,
  epoch_ros numeric NULL,
  PRIMARY KEY (pool_id, epoch_no)
);

COMMENT ON TABLE grest.pool_history_cache IS 'A history of pool performance including blocks, delegators, active stake, fees AND rewards';

CREATE OR REPLACE FUNCTION grest.pool_history_cache_update(_epoch_no_to_insert_from bigint DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  _curr_epoch bigint;
  _latest_epoch_no_in_cache bigint;
BEGIN
  IF (
    SELECT COUNT(pid) > 1
    FROM pg_stat_activity
    WHERE state = 'active' AND query ILIKE '%grest.pool_history_cache_update%'
      AND datname = (SELECT current_database())
    ) THEN
      RAISE EXCEPTION 'Previous pool_history_cache_update query still running but should have completed! Exiting...';
  END IF;

  IF (
    SELECT COUNT(key) != 1
    FROM GREST.CONTROL_TABLE
    WHERE key = 'last_active_stake_validated_epoch'
  ) THEN
    RAISE EXCEPTION 'Active stake cache not yet populated! Exiting...';
  END IF;

  SELECT COALESCE(MAX(epoch_no), 0) INTO _latest_epoch_no_in_cache FROM grest.pool_history_cache;
  -- Split into 500 epochs at a time to avoid hours spent on a single query (which can be risky if that query is killed)
  SELECT LEAST( 500 , (MAX(no) - _latest_epoch_no_in_cache) ) + _latest_epoch_no_in_cache INTO _curr_epoch FROM epoch;

  IF _epoch_no_to_insert_from IS NULL THEN
    IF _latest_epoch_no_in_cache = 0 THEN
      RAISE NOTICE 'Pool history cache table is empty, starting initial population...';
      PERFORM grest.pool_history_cache_update (0);
      RETURN;
    END IF;
    -- no-op IF we already have data up until second most recent epoch
    IF _latest_epoch_no_in_cache >= (_curr_epoch - 2) THEN
      INSERT INTO grest.control_table (key, last_value)
        VALUES ('pool_history_cache_last_updated', NOW() AT TIME ZONE 'utc')
      ON CONFLICT (key)
        DO UPDATE SET last_value = NOW() AT TIME ZONE 'utc';
      RETURN;
    END IF;
    -- IF current epoch is at least 2 ahead of latest in cache, repopulate FROM latest in cache until current-1
    _epoch_no_to_insert_from := _latest_epoch_no_in_cache;
  END IF;
  -- purge the data for the given epoch range, in theory should do nothing IF invoked only at start of new epoch
  DELETE FROM grest.pool_history_cache
  WHERE epoch_no >= _epoch_no_to_insert_from;

  RAISE NOTICE 'inserting data from % to %', _epoch_no_to_insert_from, _curr_epoch;


  INSERT INTO grest.pool_history_cache (
    select * from grest.get_pool_history_data_bulk(_epoch_no_to_insert_from::word31type, null::text [], _curr_epoch::word31type)
  );

  INSERT INTO grest.control_table (key, last_value)
    VALUES ('pool_history_cache_last_updated', NOW() AT TIME ZONE 'utc')
  ON CONFLICT (key)
    DO UPDATE SET last_value = NOW() AT TIME ZONE 'utc';

END;
$$;

COMMENT ON FUNCTION grest.pool_history_cache_update IS 'Internal function to update pool history for data FROM specified epoch until current-epoch-minus-one. Invoke WITH non-empty param for initial population, WITH empty for subsequent updates'; --noqa: LT01
