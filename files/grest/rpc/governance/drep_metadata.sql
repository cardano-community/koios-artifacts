CREATE OR REPLACE FUNCTION grest.drep_metadata(_drep_ids text [])
RETURNS TABLE (
  drep_id character varying,
  url text,
  hash text,
  json jsonb
)
LANGUAGE sql STABLE
AS $$
  SELECT
    DISTINCT ON (dh.view) dh.view AS drep_id,
    va.url,
    ENCODE(va.data_hash, 'hex') AS hash,
    ocvd.json
  FROM public.drep_hash AS dh
    INNER JOIN public.drep_registration AS dr ON dh.id = dr.drep_hash_id
    LEFT JOIN public.voting_anchor AS va ON dr.voting_anchor_id = va.id
    LEFT JOIN public.off_chain_vote_data AS ocvd ON va.id = ocvd.voting_anchor_id
  WHERE dh.view = ANY(_drep_ids)
  ORDER BY
    dh.view, dr.tx_id DESC;
$$;

COMMENT ON FUNCTION grest.drep_metadata IS 'Get bulk DRep metadata from bech32 formatted DRep IDs'; -- noqa: LT01
