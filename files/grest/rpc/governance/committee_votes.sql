CREATE OR REPLACE FUNCTION grest.committee_votes(_committee_hash text)
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
  FROM public.committee_hash ch
    INNER JOIN public.voting_procedure vp ON ch.id = vp.committee_voter
    INNER JOIN public.gov_action_proposal gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx ON gap.tx_id = tx.id
    INNER JOIN public.block b ON tx.block_id = b.id
  WHERE ch.raw = DECODE(_committee_hash, 'hex')
  ORDER BY
    block_time DESC;
$$;

COMMENT ON FUNCTION grest.committee_votes IS 'Get all committee votes cast by given committee member or collective'; -- noqa: LT01
