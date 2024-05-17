drop function if exists grest.get_pool_history_data_bulk;
CREATE OR REPLACE FUNCTION grest.get_pool_history_data_bulk(_epoch_no_to_insert_from word31type, _pool_bech32 text [] DEFAULT null, _epoch_no_until word31type DEFAULT null)
RETURNS TABLE (
  pool_id_bech32 text,
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
  _pool_ids := (SELECT array_agg(id) from pool_hash ph where ph.view = ANY(_pool_bech32));

  RETURN QUERY
  
  WITH
    blockcounts AS (
      SELECT
        sl.pool_hash_id,
        b.epoch_no,
        COUNT(*) AS block_cnt
      FROM block AS b,
        slot_leader AS sl
      WHERE b.slot_leader_id = sl.id
		    AND (_pool_bech32 is null or sl.pool_hash_id = ANY(_pool_ids))
        AND b.epoch_no >= _epoch_no_to_insert_from
        AND (_epoch_no_until is null or b.epoch_no <= _epoch_no_until) 
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
        AND (_epoch_no_until is null or act.epoch_no <= _epoch_no_until)
      	AND (_pool_bech32 is null or act.pool_id = ANY(_pool_bech32))
    ),

    delegators AS (
      SELECT
        es.pool_id,
        es.epoch_no,
        COUNT(1) AS delegator_cnt
      FROM epoch_stake AS es
      WHERE es.epoch_no >= _epoch_no_to_insert_from
        AND (_epoch_no_until is null or es.epoch_no <= _epoch_no_until)
        AND (_pool_bech32 is null or es.pool_id = ANY(_pool_ids))
      GROUP BY
        es.pool_id,
        es.epoch_no
    )

    SELECT
      actf.pool_id::text,
      actf.epoch_no,
      actf.active_stake,
      actf.active_stake_pct,
      actf.saturation_pct,
      COALESCE(b.block_cnt, 0) AS block_cnt,
      del.delegator_cnt,
      actf.pool_fee_variable::double precision,
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
      END::double precision AS member_rewards,
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
      AND actf.epoch_no = del.epoch_no;
     
END;
$$;

COMMENT ON FUNCTION grest.get_pool_history_data_bulk IS 'Pool block production and reward history from a given epoch until optional later epoch, for all or particular subset of pools'; -- noqa: LT01

CREATE OR REPLACE FUNCTION grest.pool_history(_pool_bech32 text, _epoch_no word31type DEFAULT NULL)
RETURNS TABLE (
  epoch_no bigint,
  active_stake text,
  active_stake_pct numeric,
  saturation_pct numeric,
  block_cnt bigint,
  delegator_cnt bigint,
  margin double precision,
  fixed_cost text,
  pool_fees text,
  deleg_rewards text,
  member_rewards text,
  epoch_ros numeric
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
  _curr_epoch word31type;
BEGIN
  select max(no) into _curr_epoch from epoch;

  RETURN QUERY

 SELECT x.* from (

  SELECT
    epoch_no,
    active_stake::text,
    active_stake_pct,
    saturation_pct,
    block_cnt,
    delegator_cnt,
    pool_fee_variable AS margin,
    pool_fee_fixed::text AS fixed_cost,
    pool_fees::text,
    deleg_rewards::text,
    member_rewards::text,
    COALESCE(epoch_ros, 0)
  FROM grest.pool_history_cache AS phc
  WHERE phc.pool_id = _pool_bech32 
      AND phc.epoch_no < (_curr_epoch - 2) -- temporary condition for testing, until cache table population fixed, then can be removed

  UNION 
 
  SELECT epoch_no, active_stake::text, active_stake_pct, saturation_pct, block_cnt, delegator_cnt, margin, fixed_cost::text, pool_fees::text, deleg_rewards::text, member_rewards::text, epoch_ros 
  from grest.get_pool_history_data_bulk(_curr_epoch - 2, ARRAY[_pool_bech32], _curr_epoch - 1) -- do not care about current or future epochs for history endpoint

 ) x 
  WHERE (_epoch_no is null or x.epoch_no = _epoch_no) 
  ORDER by x.epoch_no desc;

END;
$$;

COMMENT ON FUNCTION grest.pool_history IS 'Pool block production and reward history for a given epoch (or all epochs if not specified)'; -- noqa: LT01
