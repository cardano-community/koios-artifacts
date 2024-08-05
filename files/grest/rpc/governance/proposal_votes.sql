CREATE OR REPLACE FUNCTION grest.proposal_votes(_proposal_tx_hash text, _proposal_index integer)
RETURNS TABLE (
  block_time integer,
  voter_role text,
  voter text,
  voter_hex text,
  vote text,
  meta_url text,
  meta_hash text
)
LANGUAGE sql STABLE
AS $$
  SELECT z.*
    FROM (
      SELECT
        distinct on (COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view))
        EXTRACT(EPOCH FROM vote_block.time)::integer AS block_time,
        vp.voter_role,
        COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view) as voter,
        COALESCE(ENCODE(ch.raw, 'hex'), ENCODE(dh.raw, 'hex'), ENCODE(ph.hash_raw, 'hex')) as voter_hex,
        vp.vote,
        va.url,
        ENCODE(va.data_hash, 'hex')
      FROM public.voting_procedure AS vp
        INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
        INNER JOIN public.tx ON gap.tx_id = tx.id
        INNER JOIN public.tx AS vote_tx ON vp.tx_id = vote_tx.id
        INNER JOIN public.block AS vote_block ON vote_tx.block_id = vote_block.id
        LEFT JOIN public.drep_hash AS dh ON vp.drep_voter = dh.id 
        LEFT JOIN public.pool_hash AS ph ON vp.pool_voter = ph.id
        LEFT JOIN public.committee_hash AS ch ON vp.committee_voter = ch.id
        LEFT JOIN public.voting_anchor AS va ON vp.voting_anchor_id = va.id
      WHERE tx.hash = DECODE(_proposal_tx_hash, 'hex')
        AND gap.index = _proposal_index
        -- will we need a similar filters to the one below for pool and committee member retirements?
        AND (
          CASE
            WHEN dh.view IS NOT NULL THEN ((SELECT coalesce(dreg.deposit, 0) FROM drep_registration AS dreg WHERE dreg.drep_hash_id = dh.id ORDER BY id DESC LIMIT 1) >= 0)
            ELSE true
          END)
      ORDER by
        COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view),
        block_time DESC
    ) z ORDER BY block_time desc;
$$;

COMMENT ON FUNCTION grest.proposal_votes IS 'Get all votes cast on specified governance action'; -- noqa: LT01
