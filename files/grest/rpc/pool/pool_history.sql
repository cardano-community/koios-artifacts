CREATE OR REPLACE FUNCTION grest.pool_history(_pool_bech32 text, _epoch_no word31type DEFAULT null)
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

  SELECT MAX(no) INTO _curr_epoch FROM epoch;

  RETURN QUERY
    SELECT x.*
    FROM (
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
      SELECT
        epoch_no,
        active_stake::text,
        active_stake_pct,
        saturation_pct,
        block_cnt,
        delegator_cnt,
        margin,
        fixed_cost::text,
        pool_fees::text,
        deleg_rewards::text,
        member_rewards::text,
        epoch_ros 
      FROM grest.get_pool_history_data_bulk(_curr_epoch - 2, ARRAY[_pool_bech32], _curr_epoch - 1) -- do not care about current or future epochs for history endpoint
    ) x 
    WHERE (_epoch_no is null or x.epoch_no = _epoch_no) 
    ORDER by x.epoch_no desc;

END;
$$;

COMMENT ON FUNCTION grest.pool_history IS 'Pool block production and reward history for a given epoch (or all epochs if not specified)'; -- noqa: LT01
