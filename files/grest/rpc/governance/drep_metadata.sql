CREATE OR REPLACE FUNCTION grest.drep_metadata(_drep_ids text [])
RETURNS TABLE (
  drep_id text,
  hex text,
  has_script boolean,
  url character varying,
  hash text,
  json jsonb,
  bytes text,
  warning character varying,
  language character varying,
  comment character varying,
  is_valid boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
  drep_ids_raw  hash28type[];
BEGIN

  SELECT INTO drep_ids_raw ARRAY_AGG(DECODE(grest.cip129_drep_id_to_hex(n), 'hex')) FROM UNNEST(_drep_ids) AS n;

  RETURN QUERY (
    SELECT DISTINCT ON (dh.raw)
      grest.cip129_hex_to_drep_id(dh.raw, dh.has_script) AS drep_id,
      ENCODE(dh.raw, 'hex')::text AS hex,
      dh.has_script AS has_script,
      va.url,
      ENCODE(va.data_hash, 'hex') AS hash,
      ocvd.json,
      ENCODE(ocvd.bytes,'hex')::text AS bytes,
      ocvd.warning AS warning,
      ocvd.language AS language,
      ocvd.comment AS comment,
      COALESCE(ocvd.is_valid, true) AS is_valid
    FROM public.drep_hash AS dh
      INNER JOIN public.drep_registration AS dr ON dh.id = dr.drep_hash_id
      LEFT JOIN public.voting_anchor AS va ON dr.voting_anchor_id = va.id
      LEFT JOIN public.off_chain_vote_data AS ocvd ON va.id = ocvd.voting_anchor_id
    WHERE dh.raw = ANY(drep_ids_raw)
    ORDER BY
      dh.raw, dr.tx_id DESC
  );

END;
$$;

COMMENT ON FUNCTION grest.drep_metadata IS 'Get bulk DRep metadata from bech32 formatted DRep IDs'; -- noqa: LT01
