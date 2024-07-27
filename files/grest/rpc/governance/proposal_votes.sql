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
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    vp.voter_role,
    COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view),
    vp.vote
  FROM public.voting_procedure AS vp
    INNER JOIN public.gov_action_proposal gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx ON gap.tx_id = tx.id
    INNER JOIN public.block b ON tx.block_id = b.id
    LEFT JOIN drep_hash dh ON vp.drep_voter = dh.id
    LEFT JOIN pool_hash ph ON vp.pool_voter = ph.id
    LEFT JOIN committee_hash ch ON vp.committee_voter = ch.id
  WHERE tx.hash = DECODE(_tx_hash, 'hex')
  ORDER BY block_time DESC;
$$;

COMMENT ON FUNCTION grest.proposal_votes IS 'Get all votes cast on specified governance action'; -- noqa: LT01
