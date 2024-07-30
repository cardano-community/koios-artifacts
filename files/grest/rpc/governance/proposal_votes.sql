CREATE OR REPLACE FUNCTION grest.proposal_votes(_tx_hash text, _cert_index integer)
RETURNS TABLE (
  block_time integer,
  voter_role text,
  voter text,
  vote text
)
LANGUAGE sql STABLE
AS $$
  SELECT z.* from (
    SELECT
      distinct on (COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view))
      EXTRACT(EPOCH FROM vote_block.time)::integer AS block_time,
      vp.voter_role,
      COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view) as voter,
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
    -- will we need a similar filters to the one below for pool and committee member retirements?
    AND (case when dh.view is not null then ((select coalesce(dreg.deposit, 0) from drep_registration dreg where dreg.drep_hash_id = dh.id order by id desc limit 1) >= 0) else true end)
    ORDER by COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view), block_time DESC
  ) z ORDER BY block_time desc;
$$;

COMMENT ON FUNCTION grest.proposal_votes IS 'Get all votes cast on specified governance action'; -- noqa: LT01
