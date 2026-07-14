CREATE TABLE IF NOT EXISTS grest.pool_history_cache (
  pool_id bigint,
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
  pool_id bigint,
  epoch_no bigint,
  active_stake lovelace,
  active_stake_pct numeric,
  saturation_pct numeric,
  block_cnt bigint,
  delegator_cnt bigint,
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
  _pool_ids := (SELECT ARRAY_AGG(id) from pool_hash ph where ph.hash_raw = ANY(
    SELECT cardano.bech32_decode_data(pool)
    FROM UNNEST(_pool_bech32) AS pool)
  );

  IF _pool_bech32 IS NOT NULL AND _pool_ids IS NULL THEN
    RAISE EXCEPTION 'No valid pool Bech32 strings provided.';
  END IF;

  RETURN QUERY
  
  WITH
    blockcounts AS (
      SELECT
        sl.pool_hash_id,
        b.epoch_no,
        COUNT(*) AS block_cnt
      FROM block AS b
      INNER JOIN slot_leader AS sl ON b.slot_leader_id = sl.id
      WHERE (_pool_bech32 IS NULL OR sl.pool_hash_id = ANY(_pool_ids))
        AND b.epoch_no >= _epoch_no_to_insert_from
        AND (_epoch_no_until IS NULL OR b.epoch_no < _epoch_no_until)
      GROUP BY
        sl.pool_hash_id,
        b.epoch_no
    ),
    reward_totals AS (
      SELECT
        r.pool_id,
        r.earned_epoch,
        COALESCE(SUM(CASE WHEN r.type = 'leader' THEN r.amount ELSE 0 END), 0) AS leadertotal,
        COALESCE(SUM(CASE WHEN r.type = 'member' THEN r.amount ELSE 0 END), 0) AS memtotal
      FROM reward AS r
      WHERE r.type IN ('leader', 'member')
        AND (_pool_bech32 IS NULL OR r.pool_id = ANY(_pool_ids))
        AND r.earned_epoch >= _epoch_no_to_insert_from
        AND (_epoch_no_until IS NULL OR r.earned_epoch < _epoch_no_until)
      GROUP BY
        r.pool_id,
        r.earned_epoch
    ),

    epoch_info AS (
      SELECT
        e.no AS epoch_no,
        ep.optimal_pool_count,
        tot.supply::bigint AS supply,
        easc.amount AS active_stake_total
      FROM epoch AS e
      LEFT JOIN epoch_param AS ep ON ep.epoch_no = e.no
      LEFT JOIN LATERAL grest.totals(e.no) AS tot ON true
      LEFT JOIN grest.epoch_active_stake_cache AS easc ON easc.epoch_no = e.no
      WHERE e.no >= _epoch_no_to_insert_from
        AND (_epoch_no_until IS NULL OR e.no < _epoch_no_until)
    ),

    active_stake_agg AS (
      SELECT
        es.pool_id,
        es.epoch_no,
        SUM(es.amount) AS active_stake,
        COUNT(1) AS delegator_cnt
      FROM epoch_stake AS es
      WHERE es.epoch_no >= _epoch_no_to_insert_from
        AND (_epoch_no_until IS NULL OR es.epoch_no < _epoch_no_until)
        AND (_pool_bech32 IS NULL OR es.pool_id = ANY(_pool_ids))
      GROUP BY es.pool_id, es.epoch_no
    )

  SELECT
    asa.pool_id::bigint,
    asa.epoch_no::bigint,
    asa.active_stake::lovelace,
    (asa.active_stake / NULLIF(ei.active_stake_total, 0)) * 100 AS active_stake_pct,
    ROUND((asa.active_stake / NULLIF(ei.supply / NULLIF(ei.optimal_pool_count, 0), 0) * 100), 2) AS saturation_pct,
    COALESCE(b.block_cnt, 0) AS block_cnt,
    asa.delegator_cnt,
    pu.margin::double precision AS pool_fee_variable,
    pu.fixed_cost,
    CASE COALESCE(b.block_cnt, 0)
    WHEN 0 THEN
      0
    ELSE
      -- special CASE for WHEN reward information is not available yet
      CASE COALESCE(rt.leadertotal, 0) + COALESCE(rt.memtotal, 0)
        WHEN 0 THEN NULL
        ELSE
          CASE
            WHEN COALESCE(rt.leadertotal, 0) < pu.fixed_cost THEN COALESCE(rt.leadertotal, 0)
            ELSE ROUND(pu.fixed_cost + (((COALESCE(rt.memtotal, 0) + COALESCE(rt.leadertotal, 0)) - pu.fixed_cost) * pu.margin))
          END
      END
    END AS pool_fees,
    CASE COALESCE(b.block_cnt, 0)
    WHEN 0 THEN
      0
    ELSE
      -- special CASE for WHEN reward information is not available yet
      CASE COALESCE(rt.leadertotal, 0) + COALESCE(rt.memtotal, 0)
        WHEN 0 THEN NULL
      ELSE
        CASE
          WHEN COALESCE(rt.leadertotal, 0) < pu.fixed_cost THEN COALESCE(rt.memtotal, 0)
          ELSE ROUND(COALESCE(rt.memtotal, 0) + (COALESCE(rt.leadertotal, 0) - (pu.fixed_cost + (((COALESCE(rt.memtotal, 0) + COALESCE(rt.leadertotal, 0)) - pu.fixed_cost) * pu.margin))))
        END
      END
    END AS deleg_rewards,
    CASE COALESCE(b.block_cnt, 0)
      WHEN 0 THEN 0
    ELSE
      CASE COALESCE(rt.memtotal, 0)
        WHEN 0 THEN NULL
        ELSE COALESCE(rt.memtotal, 0)
      END
    END::double precision AS member_rewards,
    CASE COALESCE(b.block_cnt, 0)
      WHEN 0 THEN 0
    ELSE
      -- special CASE for WHEN reward information is not available yet
      CASE COALESCE(rt.leadertotal, 0) + COALESCE(rt.memtotal, 0)
        WHEN 0 THEN NULL
        ELSE
          CASE
            WHEN COALESCE(rt.leadertotal, 0) < pu.fixed_cost THEN ROUND((((POW((LEAST(((COALESCE(rt.memtotal, 0)) / (NULLIF(asa.active_stake, 0))), 1000) + 1), 73) - 1)) * 100)::numeric, 9)
            -- using LEAST AS a way to prevent overflow, in CASE of dodgy database data (e.g. giant rewards / tiny active stake)
            ELSE ROUND((((POW((LEAST((((COALESCE(rt.memtotal, 0) + (COALESCE(rt.leadertotal, 0) - (pu.fixed_cost + (((COALESCE(rt.memtotal, 0)
                + COALESCE(rt.leadertotal, 0)) - pu.fixed_cost) * pu.margin))))) / (NULLIF(asa.active_stake, 0))), 1000) + 1), 73) - 1)) * 100)::numeric, 9)
          END
      END
    END AS epoch_ros
  FROM active_stake_agg AS asa
  LEFT JOIN epoch_info AS ei ON asa.epoch_no = ei.epoch_no
  LEFT JOIN LATERAL (
    SELECT margin, fixed_cost
    FROM pool_update
    WHERE hash_id = asa.pool_id
      AND active_epoch_no <= asa.epoch_no
    ORDER BY active_epoch_no DESC, id DESC
    LIMIT 1
  ) AS pu ON true
  LEFT JOIN blockcounts AS b ON asa.pool_id = b.pool_hash_id
    AND asa.epoch_no = b.epoch_no
  LEFT JOIN reward_totals AS rt ON asa.pool_id = rt.pool_id
    AND asa.epoch_no = rt.earned_epoch;
     
END;
$$;

COMMENT ON FUNCTION grest.get_pool_history_data_bulk IS 'Pool block production and reward history from a given epoch until optional later epoch, for all or particular subset of pools'; -- noqa: LT01

DROP FUNCTION IF EXISTS grest.pool_history_cache_update;

CREATE OR REPLACE PROCEDURE grest.pool_history_cache_update(_epoch_no_to_insert_from bigint DEFAULT null)
LANGUAGE plpgsql
AS $$
DECLARE
  _curr_epoch bigint;
  _latest_epoch_no_in_cache bigint;
  _insert_epoch bigint;
  _batch_end_epoch bigint;
BEGIN
  IF NOT pg_try_advisory_lock(hashtext('pool_history_cache_update'::text)) THEN
    RAISE EXCEPTION 'Previous pool_history_cache_update query still running but should have completed! Exiting...';
  END IF;

  IF (
    SELECT COUNT(key) != 1
    FROM GREST.CONTROL_TABLE
    WHERE key = 'last_active_stake_validated_epoch'
  ) THEN
    PERFORM pg_advisory_unlock(hashtext('pool_history_cache_update'::text));
    RAISE EXCEPTION 'Active stake cache not yet populated! Exiting...';
  END IF;

  SELECT COALESCE(MAX(epoch_no), 0) INTO _latest_epoch_no_in_cache FROM grest.pool_history_cache;
  SELECT MAX(epoch_param.epoch_no) INTO _curr_epoch FROM public.epoch_param;

  IF _epoch_no_to_insert_from IS NULL THEN
    IF _latest_epoch_no_in_cache = 0 THEN
      RAISE NOTICE 'Pool history cache table is empty, starting initial population...';
      _epoch_no_to_insert_from := 0;
    ELSE
      -- no-op IF we already have data up until second most recent epoch
      IF _latest_epoch_no_in_cache >= _curr_epoch - 1 THEN
        INSERT INTO grest.control_table (key, last_value)
          VALUES ('pool_history_cache_last_updated', NOW() AT TIME ZONE 'utc')
        ON CONFLICT (key)
          DO UPDATE SET last_value = NOW() AT TIME ZONE 'utc';
          
        PERFORM pg_advisory_unlock(hashtext('pool_history_cache_update'::text));
        RETURN;
      END IF;
      
      -- IF current epoch is at least 2 ahead of latest in cache, repopulate FROM latest in cache
      _epoch_no_to_insert_from := _latest_epoch_no_in_cache;
    END IF;
  END IF;

  -- Process in batches of 50 epochs (smaller batch) to avoid huge transactions
  _insert_epoch := _epoch_no_to_insert_from;
  
  WHILE _insert_epoch <= _curr_epoch LOOP
    _batch_end_epoch := LEAST(_insert_epoch + 50, _curr_epoch + 1);
    
    RAISE NOTICE 'inserting data from % to %', _insert_epoch, _batch_end_epoch;

    INSERT INTO grest.pool_history_cache (
      SELECT * FROM grest.get_pool_history_data_bulk(_insert_epoch::word31type, null::text [], _batch_end_epoch::word31type)
    )
    ON CONFLICT (pool_id, epoch_no) 
    DO UPDATE SET
      active_stake = EXCLUDED.active_stake,
      active_stake_pct = EXCLUDED.active_stake_pct,
      saturation_pct = EXCLUDED.saturation_pct,
      block_cnt = EXCLUDED.block_cnt,
      delegator_cnt = EXCLUDED.delegator_cnt,
      pool_fee_variable = EXCLUDED.pool_fee_variable,
      pool_fee_fixed = EXCLUDED.pool_fee_fixed,
      pool_fees = EXCLUDED.pool_fees,
      deleg_rewards = EXCLUDED.deleg_rewards,
      member_rewards = EXCLUDED.member_rewards,
      epoch_ros = EXCLUDED.epoch_ros; 

    INSERT INTO grest.control_table (key, last_value)
      VALUES ('pool_history_cache_last_updated', NOW() AT TIME ZONE 'utc')
    ON CONFLICT (key)
      DO UPDATE SET last_value = NOW() AT TIME ZONE 'utc';

    COMMIT;
    
    _insert_epoch := _batch_end_epoch;
  END LOOP;

  PERFORM pg_advisory_unlock(hashtext('pool_history_cache_update'::text));
END;
$$;

COMMENT ON PROCEDURE grest.pool_history_cache_update IS 'Internal procedure to update pool history for data FROM specified epoch until current-epoch-minus-one. Invoke WITH non-empty param for initial population, WITH empty for subsequent updates'; --noqa: LT01
