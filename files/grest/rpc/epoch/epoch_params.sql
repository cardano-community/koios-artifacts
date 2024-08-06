CREATE OR REPLACE FUNCTION grest.epoch_params(_epoch_no numeric DEFAULT NULL)
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
  coins_per_utxo_size text,
  pvt_motion_no_confidence double precision,
  pvt_committee_normal double precision,
  pvt_committee_no_confidence double precision,
  pvt_hard_fork_initiation double precision,
  dvt_motion_no_confidence double precision,
  dvt_committee_normal double precision,
  dvt_committee_no_confidence double precision,
  dvt_update_to_constitution double precision,
  dvt_hard_fork_initiation double precision,
  dvt_p_p_network_group double precision,
  dvt_p_p_economic_group double precision,
  dvt_p_p_technical_group double precision,
  dvt_p_p_gov_group double precision,
  dvt_treasury_withdrawal double precision,
  committee_min_size word64type,
  committee_max_term_length word64type,
  gov_action_lifetime word64type,
  gov_action_deposit text,
  drep_deposit text,
  drep_activity word64type,
  pvtpp_security_group double precision,
  min_fee_ref_script_cost_per_byte double precision
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT
      ep.epoch_no,
      ep.min_fee_a AS min_fee_a,
      ep.min_fee_b AS min_fee_b,
      ep.max_block_size AS max_block_size,
      ep.max_tx_size AS max_tx_size,
      ep.max_bh_size AS max_bh_size,
      ep.key_deposit::text AS key_deposit,
      ep.pool_deposit::text AS pool_deposit,
      ep.max_epoch AS max_epoch,
      ep.optimal_pool_count AS optimal_pool_count,
      ep.influence AS influence,
      ep.monetary_expand_rate AS monetary_expand_rate,
      ep.treasury_growth_rate AS treasury_growth_rate,
      ep.decentralisation AS decentralisation,
      ENCODE(ep.extra_entropy, 'hex') AS extra_entropy,
      ep.protocol_major AS protocol_major,
      ep.protocol_minor AS protocol_minor,
      ep.min_utxo_value::text AS min_utxo_value,
      ep.min_pool_cost::text AS min_pool_cost,
      ei.p_nonce AS nonce,
      ei.p_block_hash AS block_hash,
      cm.costs AS cost_models,
      ep.price_mem AS price_mem,
      ep.price_step AS price_step,
      ep.max_tx_ex_mem AS max_tx_ex_mem,
      ep.max_tx_ex_steps AS max_tx_ex_steps,
      ep.max_block_ex_mem AS max_block_ex_mem,
      ep.max_block_ex_steps AS max_block_ex_steps,
      ep.max_val_size AS max_val_size,
      ep.collateral_percent AS collateral_percent,
      ep.max_collateral_inputs AS max_collateral_inputs,
      ep.coins_per_utxo_size::text AS coins_per_utxo_size,
      ep.pvt_motion_no_confidence,
      ep.pvt_committee_normal,
      ep.pvt_committee_no_confidence,
      ep.pvt_hard_fork_initiation,
      ep.dvt_motion_no_confidence,
      ep.dvt_committee_normal,
      ep.dvt_committee_no_confidence,
      ep.dvt_update_to_constitution,
      ep.dvt_hard_fork_initiation,
      ep.dvt_p_p_network_group,
      ep.dvt_p_p_economic_group,
      ep.dvt_p_p_technical_group,
      ep.dvt_p_p_gov_group,
      ep.dvt_treasury_withdrawal,
      ep.committee_min_size,
      ep.committee_max_term_length,
      ep.gov_action_lifetime,
      ep.gov_action_deposit::text,
      ep.drep_deposit::text,
      ep.drep_activity,
      ep.pvtpp_security_group,
      ep.min_fee_ref_script_cost_per_byte
    FROM epoch_param AS ep
    LEFT JOIN grest.epoch_info_cache AS ei ON ei.epoch_no = ep.epoch_no
    LEFT JOIN cost_model AS cm ON cm.id = ep.cost_model_id
    WHERE 
      CASE
        WHEN _epoch_no IS NULL THEN
          ep.epoch_no <= (SELECT MAX(epoch.no) FROM public.epoch)
        ELSE
          ep.epoch_no = _epoch_no
        END
    ORDER BY
      ei.epoch_no DESC;
END;
$$;

COMMENT ON FUNCTION grest.epoch_params IS 'Get the epoch parameters, all epochs if no epoch specified'; -- noqa: LT01
