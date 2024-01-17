CREATE OR REPLACE FUNCTION grest.pool_metadata(_pool_bech32_ids text [] DEFAULT null)
RETURNS TABLE (
  pool_id_bech32 character varying,
  meta_url character varying,
  meta_hash text,
  meta_json jsonb,
  pool_status text
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  SELECT DISTINCT ON (pic.pool_id_bech32)
    ph.view AS pool_id_bech32,
    pic.meta_url,
    pic.meta_hash,
    ocpd.json,
    pic.pool_status
  FROM public.pool_hash AS ph
  LEFT JOIN grest.pool_info_cache AS pic ON ph.view = pic.pool_id_bech32
  LEFT JOIN public.off_chain_pool_data AS ocpd ON ocpd.pmr_id = pic.meta_id
  WHERE
    CASE
      WHEN _pool_bech32_ids IS NULL THEN TRUE
      WHEN _pool_bech32_ids IS NOT NULL THEN pic.pool_id_bech32 = ANY(SELECT UNNEST(_pool_bech32_ids))
    END
  ORDER BY
      pic.pool_id_bech32,
      pic.tx_id DESC;
END;
$$;

COMMENT ON FUNCTION grest.pool_metadata IS 'Metadata(on & off-chain) for all pools'; -- noqa: LT01
