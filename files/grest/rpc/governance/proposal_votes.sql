CREATE OR REPLACE FUNCTION grest.proposal_votes(_tx_hash text, _cert_index integer)
RETURNS TABLE (
  block_time integer,
  voter_role text,
  voter text,
  vote text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    EXTRACT(EPOCH FROM vote_block.time)::integer AS block_time,
    vp.voter_role,
    COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view),
    vp.vote
  FROM public.voting_procedure AS vp
    INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx ON gap.tx_id = tx.id
    INNER JOIN public.tx AS vote_tx ON vp.tx_id = vote_tx.id
    INNER JOIN public.block AS vote_block ON vote_tx.block_id = vote_block.id
    LEFT JOIN public.drep_hash AS dh ON vp.drep_voter = dh.id
    LEFT JOIN public.pool_hash AS ph ON vp.pool_voter = ph.id
    LEFT JOIN public.committee_hash AS ch ON vp.committee_voter = ch.id
  WHERE tx.hash = DECODE(_tx_hash, 'hex')
  ORDER BY block_time DESC;
$$;

COMMENT ON FUNCTION grest.proposal_votes IS 'Get all votes cast on specified governance action'; -- noqa: LT01
