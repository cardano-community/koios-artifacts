-- EPOCH

CREATE OR REPLACE FUNCTION grestv0.epoch_block_protocols(_epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  proto_major word31type,
  proto_minor word31type,
  blocks bigint
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.epoch_block_protocols(_epoch_no);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.epoch_info(_epoch_no numeric DEFAULT NULL, _include_next_epoch boolean DEFAULT FALSE)
RETURNS TABLE (
  epoch_no word31type,
  out_sum text,
  fees text,
  tx_count word31type,
  blk_count word31type,
  start_time integer,
  end_time integer,
  first_block_time integer,
  last_block_time integer,
  active_stake text,
  total_rewards text,
  avg_blk_reward text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT * FROM grest.epoch_info(_epoch_no, _include_next_epoch);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.epoch_params(_epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  epoch_no word31type,
  min_fee_a word31type,
  min_fee_b word31type,
  max_block_size word31type,
  max_tx_size word31type,
  max_bh_size word31type,
  key_deposit text,
  pool_deposit text,
  max_epoch word31type,
  optimal_pool_count word31type,
  influence double precision,
  monetary_expand_rate double precision,
  treasury_growth_rate double precision,
  decentralisation double precision,
  extra_entropy text,
  protocol_major word31type,
  protocol_minor word31type,
  min_utxo_value text,
  min_pool_cost text,
  nonce text,
  block_hash text,
  cost_models jsonb,
  price_mem double precision,
  price_step double precision,
  max_tx_ex_mem word64type,
  max_tx_ex_steps word64type,
  max_block_ex_mem word64type,
  max_block_ex_steps word64type,
  max_val_size word64type,
  collateral_percent word31type,
  max_collateral_inputs word31type,
  coins_per_utxo_size text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.epoch_params(_epoch_no);
END;
$$;
