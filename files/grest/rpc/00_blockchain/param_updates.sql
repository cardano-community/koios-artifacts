CREATE OR REPLACE FUNCTION grest.param_updates()
RETURNS TABLE (
  tx_hash text,
  block_height word31type,
  block_time integer,
  epoch_no word31type,
  data jsonb
)
LANGUAGE sql STABLE
AS $$
  SELECT DISTINCT ON (pp.registered_tx_id)
    ENCODE(t.hash,'hex') AS tx_hash,
    b.block_no AS block_height,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    b.epoch_no,
    JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
        'min_fee_a', pp.min_fee_a,
        'min_fee_b', pp.min_fee_b,
        'max_block_size', pp.max_block_size,
        'max_tx_size', pp.max_tx_size,
        'max_bh_size', pp.max_bh_size,
        'key_deposit', pp.key_deposit,
        'pool_deposit', pp.pool_deposit,
        'max_epoch', pp.max_epoch,
        'optimal_pool_count', pp.optimal_pool_count,
        'influence', pp.influence,
        'monetary_expand_rate', pp.monetary_expand_rate,
        'treasury_growth_rate', pp.treasury_growth_rate,
        'decentralisation', pp.decentralisation,
        'entropy', pp.entropy,
        'protocol_major', pp.protocol_major,
        'protocol_minor', pp.protocol_minor,
        'min_utxo_value', pp.min_utxo_value,
        'min_pool_cost', pp.min_pool_cost,
        'cost_model', CM.costs,
        'price_mem', pp.price_mem,
        'price_step', pp.price_step,
        'max_tx_ex_mem', pp.max_tx_ex_mem,
        'max_tx_ex_steps', pp.max_tx_ex_steps,
        'max_block_ex_mem', pp.max_block_ex_mem,
        'max_block_ex_steps', pp.max_block_ex_steps,
        'max_val_size', pp.max_val_size,
        'collateral_percent', pp.collateral_percent,
        'max_collateral_inputs', pp.max_collateral_inputs,
        'coins_per_utxo_size', pp.coins_per_utxo_size
      )) AS data
  FROM public.param_proposal pp
    INNER JOIN tx t ON t.id = pp.registered_tx_id
    INNER JOIN block b ON t.block_id = b.id
    LEFT JOIN cost_model CM ON CM.id = pp.cost_model_id;
$$;

COMMENT ON FUNCTION grest.param_updates IS 'Parameter updates applied to the network'; -- noqa: LT01
