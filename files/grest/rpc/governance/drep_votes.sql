CREATE OR REPLACE FUNCTION grest.drep_votes(_drep_id text)
RETURNS TABLE (
  tx_hash text,
  cert_index integer,
  block_time integer,
  vote text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(tx.hash, 'hex')::text AS tx_hash,
    gap.index AS cert_index,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    vp.vote
  FROM public.drep_hash AS dh
    INNER JOIN public.voting_procedure AS vp ON dh.id = vp.drep_voter
    INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx ON gap.tx_id = tx.id
    INNER JOIN public.block AS b ON tx.block_id = b.id
  WHERE dh.view = _drep_id
  ORDER BY
    block_time DESC;
$$;

COMMENT ON FUNCTION grest.drep_votes IS 'Get all DRep votes cast from specified DRep ID'; -- noqa: LT01
