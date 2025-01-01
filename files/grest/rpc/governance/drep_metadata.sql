CREATE OR REPLACE FUNCTION grest.drep_metadata(_drep_ids text [])
RETURNS TABLE (
  drep_id text,
  hex text,
  has_script boolean,
  meta_url varchar,
  meta_hash text,
  meta_json jsonb,
  bytes text,
  warning varchar,
  language varchar,
  comment varchar,
  is_valid boolean
)
LANGUAGE plpgsql
AS $$
BEGIN

  RETURN QUERY (
    SELECT DISTINCT ON (dh.raw)
      grest.cip129_hex_to_drep_id(dh.raw, dh.has_script) AS drep_id,
      ENCODE(dh.raw, 'hex')::text AS hex,
      dh.has_script AS has_script,
      va.url AS meta_url,
      ENCODE(va.data_hash, 'hex') AS meta_hash,
      ocvd.json AS meta_json,
      ENCODE(ocvd.bytes,'hex')::text AS bytes,
      ocvd.warning AS warning,
      ocvd.language AS language,
      ocvd.comment AS comment,
      ocvd.is_valid AS is_valid
    FROM public.drep_hash AS dh
      INNER JOIN (
        SELECT
          CASE
            WHEN STARTS_WITH(n,'drep_always') THEN NULL
          ELSE
            DECODE(grest.cip129_drep_id_to_hex(n), 'hex')
          END AS hex,
          grest.cip129_drep_id_has_script(n) AS has_script
        FROM UNNEST(_drep_ids) AS n
      ) AS dip ON dip.hex = dh.raw AND dip.has_script = dh.has_script
      INNER JOIN public.drep_registration AS dr ON dh.id = dr.drep_hash_id
      LEFT JOIN public.voting_anchor AS va ON dr.voting_anchor_id = va.id
      LEFT JOIN public.off_chain_vote_data AS ocvd ON va.id = ocvd.voting_anchor_id
    WHERE dh.raw IS NOT NULL
    ORDER BY
      dh.raw, dr.tx_id DESC
  );

END;
$$;

COMMENT ON FUNCTION grest.drep_metadata IS 'Get bulk DRep metadata from bech32 formatted DRep IDs'; -- noqa: LT01
