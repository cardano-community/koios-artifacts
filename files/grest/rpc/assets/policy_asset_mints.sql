CREATE OR REPLACE FUNCTION grest.policy_asset_mints(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  asset_name_ascii text,
  fingerprint text,
  minting_tx_hash text,
  total_supply text,
  mint_cnt bigint,
  burn_cnt bigint,
  creation_time integer,
  decimals integer
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(ma.name, 'hex')::text AS asset_name,
    ENCODE(ma.name, 'escape')::text AS asset_name_ascii,
    ma.fingerprint::text,
    ENCODE(tx.hash, 'hex') AS minting_tx_hash,
    aic.total_supply::text,
    aic.mint_cnt,
    aic.burn_cnt,
    EXTRACT(EPOCH FROM aic.creation_time)::integer,
    aic.decimals
  FROM public.multi_asset AS ma
  INNER JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
  LEFT JOIN tx ON tx.id = COALESCE(aic.last_mint_meta_tx_id, aic.last_mint_tx_id)
  WHERE ma.policy = DECODE(_asset_policy, 'hex')
  ORDER BY tx.id;
$$;

COMMENT ON FUNCTION grest.policy_asset_mints IS 'Get a list of mint/burn count details for all assets minted under a policy'; -- noqa: LT01
