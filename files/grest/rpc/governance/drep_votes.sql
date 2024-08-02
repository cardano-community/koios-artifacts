CREATE OR REPLACE FUNCTION grest.drep_votes(_drep_id text)
RETURNS TABLE (
  proposal_tx_hash text,
  cert_index integer,
  vote_tx_hash text,
  block_time integer,
  vote text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(prop_tx.hash, 'hex')::text AS proposal_tx_hash,
    gap.index AS cert_index,
    ENCODE(vote_tx.hash, 'hex')::text AS vote_tx_hash,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    vp.vote
  FROM public.drep_hash AS dh
    INNER JOIN public.voting_procedure AS vp ON dh.id = vp.drep_voter
    INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx prop_tx ON gap.tx_id = prop_tx.id
    INNER JOIN public.tx vote_tx on vp.tx_id = vote_tx.id
    INNER JOIN public.block AS b ON vote_tx.block_id = b.id
  WHERE dh.view = _drep_id
  ORDER BY
    vote_tx.id DESC;
$$;

COMMENT ON FUNCTION grest.drep_votes IS 'Get all DRep votes cast from specified DRep ID'; -- noqa: LT01
