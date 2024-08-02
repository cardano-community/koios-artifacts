CREATE OR REPLACE FUNCTION grest.committee_votes(_committee_hash text)
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
  FROM public.committee_hash AS ch
    INNER JOIN public.voting_procedure AS vp ON ch.id = vp.committee_voter
    INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx prop_tx ON gap.tx_id = prop_tx.id
    INNER JOIN public.tx vote_tx on vp.tx_id = vote_tx.id
    INNER JOIN public.block AS b ON vote_tx.block_id = b.id
  WHERE ch.raw = DECODE(_committee_hash, 'hex')
  ORDER BY
    vote_tx.id DESC;
$$;

COMMENT ON FUNCTION grest.committee_votes IS 'Get all committee votes cast by given committee member or collective'; -- noqa: LT01
