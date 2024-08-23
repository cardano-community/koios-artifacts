CREATE OR REPLACE FUNCTION grest.pool_votes(_pool_bech32 text)
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
  FROM public.pool_hash ph
    INNER JOIN public.voting_procedure AS vp ON ph.id = vp.pool_voter
    INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx prop_tx ON gap.tx_id = prop_tx.id
    INNER JOIN public.tx vote_tx on vp.tx_id = vote_tx.id
    INNER JOIN public.block AS b ON vote_tx.block_id = b.id
    LEFT JOIN public.voting_anchor AS va ON vp.voting_anchor_id = va.id
  WHERE ph.view = _pool_bech32
  ORDER BY
    vote_tx.id DESC;
$$;

COMMENT ON FUNCTION grest.pool_votes IS 'Get all SPO votes cast for a given pool'; -- noqa: LT01
