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

BEGIN
  RETURN QUERY
  SELECT
    epoch_no,
    active_stake::text,
    active_stake_pct,
    saturation_pct,
    block_cnt,
    delegator_cnt,
    pool_fee_variable AS margin,
    COALESCE(pool_fee_fixed, 0)::text AS fixed_cost,
    COALESCE(pool_fees, 0)::text,
    COALESCE(deleg_rewards, 0)::text,
    COALESCE(member_rewards, 0)::text,
    COALESCE(epoch_ros, 0)
  FROM grest.pool_history_cache AS phc
  WHERE phc.pool_id = _pool_bech32 and
    (_epoch_no IS NULL OR
        phc.epoch_no = _epoch_no)
   ORDER by phc.epoch_no desc;
END;
$$;

COMMENT ON FUNCTION grest.pool_history IS 'Pool block production and reward history for a given epoch (or all epochs if not specified)'; -- noqa: LT01
