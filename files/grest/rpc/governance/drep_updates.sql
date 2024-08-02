CREATE OR REPLACE FUNCTION grest.drep_updates(_drep_id text DEFAULT NULL)
RETURNS TABLE (
  drep_id character varying,
  hex text,
  update_tx_hash text,
  cert_index integer,
  block_time integer,
  action text,
  deposit text,
  meta_url text,
  meta_hash text,
  meta_json jsonb
)
LANGUAGE sql STABLE
AS $$
  SELECT
    dh.view AS drep_id,
    ENCODE(dh.raw, 'hex')::text AS hex,
    ENCODE(tx.hash, 'hex')::text AS update_tx_hash,
    dr.cert_index,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    CASE
      WHEN dr.deposit IS NULL THEN 'updated'
      WHEN dr.deposit > 0 THEN 'registered'
      ELSE 'deregistered'
    END AS action,
    dr.deposit,
    va.url AS meta_url,
    ENCODE(va.data_hash, 'hex') AS meta_hash,
    ocvd.json AS meta_json
  FROM public.drep_hash AS dh
    INNER JOIN public.drep_registration AS dr ON dh.id = dr.drep_hash_id
    INNER JOIN public.tx ON dr.tx_id = tx.id
    INNER JOIN public.block AS b ON tx.block_id = b.id
    LEFT JOIN public.voting_anchor AS va ON dr.voting_anchor_id = va.id
    LEFT JOIN public.off_chain_vote_data AS ocvd ON va.id = ocvd.voting_anchor_id
  WHERE
    CASE
      WHEN _drep_id IS NULL THEN TRUE
      ELSE dh.view = _drep_id
    END
  ORDER BY
    block_time DESC;
$$;

COMMENT ON FUNCTION grest.drep_updates IS 'Return all DRep updates for all DReps or only updates for specific DRep if specified'; -- noqa: LT01
