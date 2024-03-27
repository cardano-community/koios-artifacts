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
  -- Split into 100 epochs at a time to avoid hours spent on a single query (which can be risky if that query is killed)
  SELECT LEAST( 500 , (MAX(no) - _latest_epoch_no_in_cache) ) + _latest_epoch_no_in_cache INTO _curr_epoch FROM epoch;

  IF _epoch_no_to_insert_from IS NULL THEN
    IF _latest_epoch_no_in_cache = 0 THEN
      RAISE NOTICE 'Pool history cache table is empty, starting initial population...';
      PERFORM grest.pool_history_cache_update (0);
      RETURN;
    END IF;
    -- no-op IF we already have data up until second most recent epoch
    IF _latest_epoch_no_in_cache >= (_curr_epoch - 1) THEN
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

  INSERT INTO grest.pool_history_cache (
  WITH
    blockcounts AS (
      SELECT
        sl.pool_hash_id,
        b.epoch_no,
        COUNT(*) AS block_cnt
      FROM block AS b,
        slot_leader AS sl
      WHERE b.slot_leader_id = sl.id
        AND b.epoch_no >= _epoch_no_to_insert_from
      GROUP BY
        sl.pool_hash_id,
        b.epoch_no
    ),

    leadertotals AS (
      SELECT
        r.pool_id,
        r.earned_epoch,
        COALESCE(SUM(r.amount), 0) AS leadertotal
      FROM reward AS r
      WHERE r.type = 'leader'
        AND r.earned_epoch >= _epoch_no_to_insert_from
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
        AND r.earned_epoch >= _epoch_no_to_insert_from
      GROUP BY
        r.pool_id,
        r.earned_epoch
    ),

    activeandfees AS (
      SELECT
        act.pool_id,
        act.epoch_no,
        act.amount AS active_stake,
        (
          SELECT margin
          FROM
            pool_update
          WHERE
            id = (
              SELECT MAX(pup2.id)
              FROM pool_hash AS ph,
                pool_update AS pup2
              WHERE pup2.hash_id = ph.id
                AND ph.view = act.pool_id
                AND pup2.active_epoch_no <= act.epoch_no
            )
        ) AS pool_fee_variable,
        (
          SELECT fixed_cost
          FROM pool_update
          WHERE id = (
              SELECT MAX(pup2.id)
              FROM pool_update AS pup2,
                pool_hash AS ph
              WHERE ph.view = act.pool_id
                AND pup2.hash_id = ph.id
                AND pup2.active_epoch_no <= act.epoch_no)
        ) AS pool_fee_fixed,
        (act.amount / (
          SELECT NULLIF(act.amount, 0)
          FROM grest.epoch_active_stake_cache AS easc
          WHERE easc.epoch_no = act.epoch_no
          )
        ) * 100 AS active_stake_pct,
        ROUND(
          (act.amount / (
            SELECT supply::bigint / (
                SELECT ep.optimal_pool_count
                FROM epoch_param AS ep
                WHERE ep.epoch_no = act.epoch_no
              )
            FROM grest.totals (act.epoch_no)
            ) * 100
          ), 2
        ) AS saturation_pct
      FROM grest.pool_active_stake_cache AS act
      WHERE act.epoch_no >= _epoch_no_to_insert_from
        AND act.epoch_no <= _curr_epoch
    ),

    delegators AS (
      SELECT
        es.pool_id,
        es.epoch_no,
        COUNT(1) AS delegator_cnt
      FROM epoch_stake AS es
      WHERE es.epoch_no >= _epoch_no_to_insert_from
        AND es.epoch_no <= _curr_epoch
      GROUP BY
        es.pool_id,
        es.epoch_no
    )

    SELECT
      ph.view AS pool_id,
      actf.epoch_no,
      actf.active_stake,
      actf.active_stake_pct,
      actf.saturation_pct,
      COALESCE(b.block_cnt, 0) AS block_cnt,
      del.delegator_cnt,
      actf.pool_fee_variable,
      actf.pool_fee_fixed,
      -- for debugging: m.memtotal,
      -- for debugging: l.leadertotal,
      CASE COALESCE(b.block_cnt, 0)
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
      CASE COALESCE(b.block_cnt, 0)
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
      CASE COALESCE(b.block_cnt, 0)
        WHEN 0 THEN 0
      ELSE
        CASE COALESCE(m.memtotal, 0)
          WHEN 0 THEN NULL
          ELSE COALESCE(m.memtotal, 0)
        END
      END AS member_rewards,
      CASE COALESCE(b.block_cnt, 0)
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
    LEFT JOIN blockcounts AS b ON ph.id = b.pool_hash_id
      AND actf.epoch_no = b.epoch_no
    LEFT JOIN leadertotals AS l ON ph.id = l.pool_id
      AND actf.epoch_no = l.earned_epoch
    LEFT JOIN membertotals AS m ON ph.id = m.pool_id
      AND actf.epoch_no = m.earned_epoch
    LEFT JOIN delegators AS del ON ph.id = del.pool_id
      AND actf.epoch_no = del.epoch_no
  );

  INSERT INTO grest.control_table (key, last_value)
    VALUES ('pool_history_cache_last_updated', NOW() AT TIME ZONE 'utc')
  ON CONFLICT (key)
    DO UPDATE SET last_value = NOW() AT TIME ZONE 'utc';

END;
$$;

COMMENT ON FUNCTION grest.pool_history_cache_update IS 'Internal function to update pool history for data FROM specified epoch until current-epoch-minus-one. Invoke WITH non-empty param for initial population, WITH empty for subsequent updates'; --noqa: LT01
