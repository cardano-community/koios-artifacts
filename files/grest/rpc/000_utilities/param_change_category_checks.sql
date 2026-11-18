CREATE OR REPLACE FUNCTION grest.has_security_param(description jsonb)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE

BEGIN

  -- Parameters in the ledger's SecurityGroup, which need an SPO vote. Line 1 is Conway; lines 2 to 5 add Dijkstra's
  -- reference script, Leios, Peras and reference input parameters. maxPledgeLeverage and minPoolMargin are not in it.
  RETURN jsonb_path_exists(description, '$.** ? (exists(@."maxBlockBodySize") || exists(@."maxTxSize") || exists(@."maxBlockHeaderSize") || exists(@."maxValueSize") || exists(@."maxBlockExecutionUnits") || exists(@."txFeePerByte") || exists(@."txFeeFixed") || exists(@."utxoCostPerByte") || exists(@."govActionDeposit") || exists(@."minFeeRefScriptCostPerByte")
    || exists(@."maxRefScriptSizePerBlock") || exists(@."maxRefScriptSizePerTx") || exists(@."refScriptCostStride") || exists(@."refScriptCostMultiplier")
    || exists(@."leiosAnnouncementPeriodLength") || exists(@."leiosVotePeriodLength") || exists(@."leiosDiffusionPeriodLength") || exists(@."leiosCommitteeSize") || exists(@."leiosQuorumStakeThreshold") || exists(@."maxEndorserBlockReferencesSize") || exists(@."maxEndorserBlockTxsSize") || exists(@."maxEndorserBlockExecutionUnits") || exists(@."maxRefScriptSizePerEndorserBlock")
    || exists(@."perasMinCandidateBlockAge") || exists(@."perasHealingFactor") || exists(@."perasCertBoost") || exists(@."perasTargetCommitteeSize") || exists(@."perasBootstrapRound") || exists(@."perasQuorumThresholdSafetyMargin")
    || exists(@."refInputsCostPerMultiAssetPolicy") || exists(@."refInputsCostPerDatumByte"))');

END;
$$;

COMMENT ON FUNCTION grest.has_security_param IS 'Returns a boolean to indicate whether a given gov action proposal description contains at least one security parameter'; --noqa: LT01
