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

CREATE OR REPLACE FUNCTION grest.get_pool_history_data_bulk(_epoch_no_to_insert_from word31type, _pool_bech32 text [] DEFAULT null, _epoch_no_until word31type DEFAULT null)
RETURNS TABLE (
  pool_id_bech32 text,
  epoch_no bigint,
  active_stake lovelace,
  active_stake_pct numeric,
  saturation_pct numeric,
  block_cnt numeric,
  delegator_cnt numeric,
  margin double precision,
  fixed_cost lovelace,
  pool_fees double precision,
  deleg_rewards double precision,
  member_rewards double precision,
  epoch_ros numeric
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
  _pool_ids bigint [];
BEGIN
  _pool_ids := (SELECT array_agg(id) from pool_hash ph where ph.view = ANY(_pool_bech32));

  RETURN QUERY
  
  WITH
    leadertotals AS (
      SELECT
        r.pool_id,
        r.earned_epoch,
        COALESCE(SUM(r.amount), 0) AS leadertotal
      FROM reward AS r
      WHERE r.type = 'leader'
        AND (_pool_bech32 is null or r.pool_id = ANY(_pool_ids))
        AND r.earned_epoch >= _epoch_no_to_insert_from
        AND (_epoch_no_until is null or r.earned_epoch <= _epoch_no_until)
      GROUP BY
        r.pool_id,
        r.earned_epoch
    ),

    membertotals AS (
      SELECT
        r.pool_id,
        r.earned_epoch,
        COALESCE(SUM(r.amount), 0) AS memtotal
      FROM reward AS r
      WHERE r.type = 'member'
        AND (_pool_bech32 is null or r.pool_id = ANY(_pool_ids))
        AND r.earned_epoch >= _epoch_no_to_insert_from
        AND (_epoch_no_until is null or r.earned_epoch <= _epoch_no_until)
      GROUP BY
        r.pool_id,
        r.earned_epoch
    ),

    activeandfees AS (
      SELECT
        ph.view AS pool_id,
        ps.epoch_no,
        ps.stake AS active_stake,
        ps.number_of_blocks AS block_cnt,
        COALESCE(ps.number_of_delegators, 0) AS delegator_cnt,
        (
          SELECT margin
          FROM pool_update
          WHERE id = (
              SELECT MAX(pup2.id)
              FROM pool_update AS pup2
              WHERE pup2.hash_id = ps.pool_hash_id
                AND pup2.active_epoch_no <= ps.epoch_no
           )
        ) AS pool_fee_variable,
        (
          SELECT fixed_cost
          FROM pool_update
          WHERE id = (
              SELECT MAX(pup2.id)
              FROM pool_update AS pup2
              WHERE pup2.hash_id = ps.pool_hash_id
                AND pup2.active_epoch_no <= ps.epoch_no)
        ) AS pool_fee_fixed,
        (ps.stake / (
          SELECT NULLIF(easc.amount, 0)
          FROM grest.epoch_active_stake_cache AS easc
          WHERE easc.epoch_no = ps.epoch_no
          )
        ) * 100 AS active_stake_pct,
        ROUND(
          (ps.stake / (
            SELECT supply::bigint / (
                SELECT ep.optimal_pool_count
                FROM epoch_param AS ep
                WHERE ep.epoch_no = ps.epoch_no
              )
            FROM grest.totals (ps.epoch_no)
            ) * 100
          ), 2
        ) AS saturation_pct
      FROM pool_stat AS ps
        INNER JOIN pool_hash AS ph ON ps.pool_hash_id = ph.id
      WHERE ps.epoch_no >= _epoch_no_to_insert_from
        AND (_epoch_no_until is null or ps.epoch_no < _epoch_no_until)
        AND (_pool_bech32 is null or ph.view = ANY(_pool_bech32))
      GROUP BY ps.pool_hash_id, ph.view, ps.epoch_no, ps.stake, ps.number_of_blocks, ps.number_of_delegators
    )

    SELECT
      actf.pool_id::text,
      actf.epoch_no::bigint,
      actf.active_stake::lovelace,
      actf.active_stake_pct,
      actf.saturation_pct,
      COALESCE(actf.block_cnt, 0) AS block_cnt,
      actf.delegator_cnt,
      actf.pool_fee_variable::double precision,
      actf.pool_fee_fixed,
      -- for debugging: m.memtotal,
      -- for debugging: l.leadertotal,
      CASE COALESCE(actf.block_cnt, 0)
      WHEN 0 THEN
        0
      ELSE
        -- special CASE for WHEN reward information is not available yet
        CASE COALESCE(l.leadertotal, 0) + COALESCE(m.memtotal, 0)
          WHEN 0 THEN NULL
          ELSE
            CASE
              WHEN COALESCE(l.leadertotal, 0) < actf.pool_fee_fixed THEN COALESCE(l.leadertotal, 0)
              ELSE ROUND(actf.pool_fee_fixed + (((COALESCE(m.memtotal, 0) + COALESCE(l.leadertotal, 0)) - actf.pool_fee_fixed) * actf.pool_fee_variable))
            END
        END
      END AS pool_fees,
      CASE COALESCE(actf.block_cnt, 0)
      WHEN 0 THEN
        0
      ELSE
        -- special CASE for WHEN reward information is not available yet
        CASE COALESCE(l.leadertotal, 0) + COALESCE(m.memtotal, 0)
          WHEN 0 THEN NULL
        ELSE
          CASE
            WHEN COALESCE(l.leadertotal, 0) < actf.pool_fee_fixed THEN COALESCE(m.memtotal, 0)
            ELSE ROUND(COALESCE(m.memtotal, 0) + (COALESCE(l.leadertotal, 0) - (actf.pool_fee_fixed + (((COALESCE(m.memtotal, 0) + COALESCE(l.leadertotal, 0)) - actf.pool_fee_fixed) * actf.pool_fee_variable))))
          END
        END
      END AS deleg_rewards,
      CASE COALESCE(actf.block_cnt, 0)
        WHEN 0 THEN 0
      ELSE
        CASE COALESCE(m.memtotal, 0)
          WHEN 0 THEN NULL
          ELSE COALESCE(m.memtotal, 0)
        END
      END::double precision AS member_rewards,
      CASE COALESCE(actf.block_cnt, 0)
        WHEN 0 THEN 0
      ELSE
        -- special CASE for WHEN reward information is not available yet
        CASE COALESCE(l.leadertotal, 0) + COALESCE(m.memtotal, 0)
          WHEN 0 THEN NULL
          ELSE
            CASE
              WHEN COALESCE(l.leadertotal, 0) < actf.pool_fee_fixed THEN ROUND((((POW((LEAST(((COALESCE(m.memtotal, 0)) / (NULLIF(actf.active_stake, 0))), 1000) + 1), 73) - 1)) * 100)::numeric, 9)
              -- using LEAST AS a way to prevent overflow, in CASE of dodgy database data (e.g. giant rewards / tiny active stake)
              ELSE ROUND((((POW((LEAST((((COALESCE(m.memtotal, 0) + (COALESCE(l.leadertotal, 0) - (actf.pool_fee_fixed + (((COALESCE(m.memtotal, 0)
                  + COALESCE(l.leadertotal, 0)) - actf.pool_fee_fixed) * actf.pool_fee_variable))))) / (NULLIF(actf.active_stake, 0))), 1000) + 1), 73) - 1)) * 100)::numeric, 9)
            END
        END
      END AS epoch_ros
    FROM pool_hash AS ph
    INNER JOIN activeandfees AS actf ON actf.pool_id = ph.view
    LEFT JOIN leadertotals AS l ON ph.id = l.pool_id
      AND actf.epoch_no = l.earned_epoch
    LEFT JOIN membertotals AS m ON ph.id = m.pool_id
      AND actf.epoch_no = m.earned_epoch;
     
END;
$$;

COMMENT ON FUNCTION grest.get_pool_history_data_bulk IS 'Pool block production and reward history from a given epoch until optional later epoch, for all or particular subset of pools'; -- noqa: LT01

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
