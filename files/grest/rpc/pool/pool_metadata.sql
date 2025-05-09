CREATE OR REPLACE FUNCTION grest.pool_metadata(_pool_bech32_ids text [] DEFAULT null)
RETURNS TABLE (
  pool_id_bech32 varchar,
  meta_url varchar,
  meta_hash text,
  meta_json jsonb
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  SELECT DISTINCT ON (ph.id)
    cardano.bech32_encode('pool', ph.hash_raw)::varchar AS pool_id_bech32,
    pmr.url AS meta_url,
    ENCODE(pmr.hash, 'hex') AS meta_hash,
    ocpd.json AS meta_json
  FROM public.pool_hash AS ph
  LEFT JOIN public.off_chain_pool_data AS ocpd ON ocpd.pool_id = ph.id
  LEFT JOIN public.pool_metadata_ref AS pmr ON pmr.id = ocpd.pmr_id
  WHERE
    CASE
      WHEN _pool_bech32_ids IS NULL THEN TRUE
      WHEN _pool_bech32_ids IS NOT NULL THEN ph.hash_raw = ANY(
        SELECT cardano.bech32_decode_data(p)
        FROM UNNEST(_pool_bech32_ids) AS p)
    END
  ORDER BY ph.id,
      pmr.registered_tx_id DESC;
END;
$$;

COMMENT ON FUNCTION grest.pool_metadata IS 'Metadata(on & off-chain) for all pools'; -- noqa: LT01
