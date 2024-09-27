CREATE TABLE IF NOT EXISTS grest.pool_active_stake_cache (
  pool_id bigint NOT NULL,
  epoch_no bigint NOT NULL,
  amount lovelace NOT NULL,
  PRIMARY KEY (pool_id, epoch_no)
);

CREATE TABLE IF NOT EXISTS grest.epoch_active_stake_cache (
  epoch_no bigint NOT NULL,
  amount lovelace NOT NULL,
  PRIMARY KEY (epoch_no)
);

CREATE OR REPLACE FUNCTION grest.active_stake_cache_update_check()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  _latest_epoch_stake integer;
  _last_active_stake_validated_epoch text;
BEGIN
  -- Get Last Active Stake Validated Epoch
  SELECT last_value INTO _last_active_stake_validated_epoch
  FROM grest.control_table
  WHERE key = 'last_active_stake_validated_epoch';
  -- Get Current Epoch
  SELECT MAX(epoch_no) INTO _latest_epoch_stake
    FROM epoch_stake_progress
    WHERE completed='t';
  RAISE NOTICE 'Latest epoch in epoch_stake: %', _latest_epoch_stake;
  RAISE NOTICE 'Latest epoch in active stake cache: %', COALESCE(_last_active_stake_validated_epoch::integer, '0');
  IF _latest_epoch_stake::integer > COALESCE(_last_active_stake_validated_epoch,'0')::integer THEN
    RAISE NOTICE 'Running update for epoch: %', _latest_epoch_stake;
    PERFORM grest.active_stake_cache_update(_latest_epoch_stake);
  ELSE
    RAISE NOTICE 'Skipping! Active Stake cache is already up to date with DB!';
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.active_stake_cache_update_check IS 'Internal function to determine whether active stake cache should be updated'; -- noqa: LT01

CREATE OR REPLACE FUNCTION grest.active_stake_cache_update(_epoch_no integer)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  _last_active_stake_validated_epoch integer;
BEGIN
    -- CHECK PREVIOUS QUERY FINISHED RUNNING
    IF (
      SELECT COUNT(pid) > 1
      FROM pg_stat_activity
      WHERE state = 'active'
        AND query ILIKE '%grest.active_stake_cache_update(%'
        AND datname = (SELECT current_database())
    ) THEN
      RAISE EXCEPTION
        'Previous query still running but should have completed! Exiting...';
    END IF;
    -- GET PREVIOUS RUN's epoch_no
    SELECT COALESCE(
      (SELECT last_value::integer
        FROM grest.control_table
        WHERE key = 'last_active_stake_validated_epoch'), _epoch_no - 3) INTO _last_active_stake_validated_epoch;
    -- POOL ACTIVE STAKE CACHE
    INSERT INTO grest.pool_active_stake_cache
      SELECT
        epoch_stake.pool_id AS pool_id,
        epoch_stake.epoch_no,
        SUM(epoch_stake.amount) AS amount
      FROM public.epoch_stake
      WHERE epoch_stake.epoch_no >= _last_active_stake_validated_epoch
        AND epoch_stake.epoch_no <= _epoch_no
      GROUP BY
        epoch_stake.pool_id,
        epoch_stake.epoch_no
    ON CONFLICT (
      pool_id,
      epoch_no
    ) DO UPDATE
      SET amount = excluded.amount
      WHERE pool_active_stake_cache.amount IS DISTINCT FROM excluded.amount;
    
    -- Active stake older than active stake can already be captured from pool history cache
    DELETE FROM grest.pool_active_stake_cache
      WHERE epoch_no < _last_active_stake_validated_epoch;

    -- EPOCH ACTIVE STAKE CACHE
    INSERT INTO grest.epoch_active_stake_cache
      SELECT
        epoch_stake.epoch_no,
        SUM(epoch_stake.amount) AS amount
      FROM public.epoch_stake
      WHERE epoch_stake.epoch_no >= COALESCE(
          (SELECT last_value::integer
            FROM grest.control_table
            WHERE key = 'last_active_stake_validated_epoch'), 0)
        AND epoch_stake.epoch_no <= _epoch_no
      GROUP BY epoch_stake.epoch_no
      ON CONFLICT (epoch_no) DO UPDATE
        SET amount = excluded.amount
        WHERE epoch_active_stake_cache.amount IS DISTINCT FROM excluded.amount;

    -- CONTROL TABLE ENTRY
    PERFORM grest.update_control_table(
      'last_active_stake_validated_epoch',
      _epoch_no::text
    );
  END;
$$;

COMMENT ON FUNCTION grest.active_stake_cache_update IS 'Internal function to update active stake cache (epoch, pool, and account tables).'; -- noqa: LT01
