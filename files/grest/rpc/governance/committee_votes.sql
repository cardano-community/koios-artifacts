CREATE OR REPLACE FUNCTION grest.committee_votes(_committee_hash text DEFAULT NULL)
RETURNS TABLE (
  proposal_tx_hash text,
  proposal_index integer,
  vote_tx_hash text,
  block_time integer,
  vote text,
  meta_url text,
  meta_hash text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(prop_tx.hash, 'hex'),
    gap.index,
    ENCODE(vote_tx.hash, 'hex'),
    EXTRACT(EPOCH FROM b.time)::integer,
    vp.vote,
    va.url,
    ENCODE(va.data_hash, 'hex')
  FROM public.committee_hash AS ch
    INNER JOIN public.voting_procedure AS vp ON ch.id = vp.committee_voter
    INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx prop_tx ON gap.tx_id = prop_tx.id
    INNER JOIN public.tx vote_tx on vp.tx_id = vote_tx.id
    INNER JOIN public.block AS b ON vote_tx.block_id = b.id
    LEFT JOIN public.voting_anchor AS va ON vp.voting_anchor_id = va.id
  WHERE
    CASE
      WHEN _committee_hash IS NULL THEN TRUE
      ELSE ch.raw = DECODE(_committee_hash, 'hex')
    END
  ORDER BY
    vote_tx.id DESC;
$$;

COMMENT ON FUNCTION grest.committee_votes IS 'Get all committee votes cast by given committee member or collective'; -- noqa: LT01
