CREATE OR REPLACE FUNCTION grest.pool_history (_pool_bech32 text, _epoch_no word31type DEFAULT NULL)
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
    epoch_ros numeric
  )
  LANGUAGE plpgsql
  AS $$
  #variable_conflict use_column
DECLARE

BEGIN

  RETURN QUERY
  SELECT    epoch_no, active_stake::text, active_stake_pct, saturation_pct, block_cnt,
            delegator_cnt, pool_fee_variable as margin, coalesce(pool_fee_fixed, 0)::text as fixed_cost,
            coalesce(pool_fees, 0)::text, coalesce(deleg_rewards, 0)::text, coalesce(epoch_ros, 0)
  FROM grest.pool_history_cache phc
  WHERE phc.pool_id = _pool_bech32 and 
    (_epoch_no is null or 
        phc.epoch_no = _epoch_no)
   ORDER by phc.epoch_no desc;

END;
$$;

COMMENT ON FUNCTION grest.pool_history IS 'Pool block production and reward history for a given epoch (or all epochs if not specified)';
