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
    JSONB_STRIP_NULLS(
      -- Dijkstra parameters (db-sync dijkstra-0-0). A NULL means the proposal leaves the parameter as it is.
      JSONB_BUILD_OBJECT(
        'max_ref_script_size_per_block', pp.max_ref_script_size_per_block,
        'max_ref_script_size_per_tx', pp.max_ref_script_size_per_tx,
        'ref_script_cost_stride', pp.ref_script_cost_stride,
        'ref_script_cost_multiplier', pp.ref_script_cost_multiplier,
        'max_pledge_leverage', pp.max_pledge_leverage,
        'min_pool_margin', pp.min_pool_margin,
        'leios_announcement_period_length', pp.leios_announcement_period_length,
        'leios_vote_period_length', pp.leios_vote_period_length,
        'leios_diffusion_period_length', pp.leios_diffusion_period_length,
        'leios_committee_size', pp.leios_committee_size,
        'leios_quorum_stake_threshold', pp.leios_quorum_stake_threshold,
        'leios_max_eb_references_size', pp.leios_max_eb_references_size,
        'leios_max_eb_txs_size', pp.leios_max_eb_txs_size,
        'leios_max_eb_ex_mem', pp.leios_max_eb_ex_mem,
        'leios_max_eb_ex_steps', pp.leios_max_eb_ex_steps,
        'leios_max_ref_script_size_per_eb', pp.leios_max_ref_script_size_per_eb,
        'peras_min_candidate_block_age', pp.peras_min_candidate_block_age,
        'peras_healing_factor', pp.peras_healing_factor,
        'peras_cert_boost', pp.peras_cert_boost,
        'peras_target_committee_size', pp.peras_target_committee_size,
        'peras_bootstrap_round', pp.peras_bootstrap_round,
        'peras_quorum_threshold_safety_margin', pp.peras_quorum_threshold_safety_margin,
        'ref_inputs_cost_per_multi_asset_policy', pp.ref_inputs_cost_per_multi_asset_policy,
        'ref_inputs_cost_per_datum_byte', pp.ref_inputs_cost_per_datum_byte
      ) || JSONB_BUILD_OBJECT(
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
      -- Conway parameters, in an object of their own because JSONB_BUILD_OBJECT takes at most 100 arguments
      ) || JSONB_BUILD_OBJECT(
        'pvt_motion_no_confidence', pp.pvt_motion_no_confidence,
        'pvt_committee_normal', pp.pvt_committee_normal,
        'pvt_committee_no_confidence', pp.pvt_committee_no_confidence,
        'pvt_hard_fork_initiation', pp.pvt_hard_fork_initiation,
        'pvtpp_security_group', pp.pvtpp_security_group,
        'dvt_motion_no_confidence', pp.dvt_motion_no_confidence,
        'dvt_committee_normal', pp.dvt_committee_normal,
        'dvt_committee_no_confidence', pp.dvt_committee_no_confidence,
        'dvt_update_to_constitution', pp.dvt_update_to_constitution,
        'dvt_hard_fork_initiation', pp.dvt_hard_fork_initiation,
        'dvt_p_p_network_group', pp.dvt_p_p_network_group,
        'dvt_p_p_economic_group', pp.dvt_p_p_economic_group,
        'dvt_p_p_technical_group', pp.dvt_p_p_technical_group,
        'dvt_p_p_gov_group', pp.dvt_p_p_gov_group,
        'dvt_treasury_withdrawal', pp.dvt_treasury_withdrawal,
        'committee_min_size', pp.committee_min_size,
        'committee_max_term_length', pp.committee_max_term_length,
        'gov_action_lifetime', pp.gov_action_lifetime,
        'gov_action_deposit', pp.gov_action_deposit,
        'drep_deposit', pp.drep_deposit,
        'drep_activity', pp.drep_activity,
        'min_fee_ref_script_cost_per_byte', pp.min_fee_ref_script_cost_per_byte
      )) AS data
  FROM public.param_proposal pp
    INNER JOIN tx t ON t.id = pp.registered_tx_id
    INNER JOIN block b ON t.block_id = b.id
    LEFT JOIN cost_model CM ON CM.id = pp.cost_model_id;
$$;

COMMENT ON FUNCTION grest.param_updates IS 'Parameter updates applied to the network'; -- noqa: LT01
