CREATE OR REPLACE FUNCTION grest.drep_metadata(_drep_ids text [])
RETURNS TABLE (
  drep_id character varying,
  hex text,
  url text,
  hash text,
  json jsonb,
  bytes text,
  warning text,
  language text,
  comment text,
  is_valid boolean
)
LANGUAGE sql STABLE
AS $$
  SELECT
    DISTINCT ON (dh.view) dh.view AS drep_id,
    ENCODE(dh.raw, 'hex')::text AS hex,
    va.url,
    ENCODE(va.data_hash, 'hex') AS hash,
    ocvd.json,
    ENCODE(ocvd.bytes,'hex')::text AS bytes,
    ocvd.warning AS warning,
    ocvd.language AS language,
    ocvd.comment AS comment,
    COALESCE(is_valid, true) AS is_valid
  FROM public.drep_hash AS dh
    INNER JOIN public.drep_registration AS dr ON dh.id = dr.drep_hash_id
    LEFT JOIN public.voting_anchor AS va ON dr.voting_anchor_id = va.id
    LEFT JOIN public.off_chain_vote_data AS ocvd ON va.id = ocvd.voting_anchor_id
  WHERE dh.view = ANY(_drep_ids)
  ORDER BY
    dh.view, dr.tx_id DESC;
$$;

COMMENT ON FUNCTION grest.drep_metadata IS 'Get bulk DRep metadata from bech32 formatted DRep IDs'; -- noqa: LT01
