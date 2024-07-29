CREATE OR REPLACE FUNCTION grest.pool_votes(_pool_bech32 text)
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
  FROM public.pool_hash ph
    INNER JOIN public.voting_procedure AS vp ON ph.id = vp.pool_voter
    INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx ON gap.tx_id = tx.id
    INNER JOIN public.block AS b ON tx.block_id = b.id
  WHERE ph.view = _pool_bech32
  ORDER BY
    block_time DESC;
$$;

COMMENT ON FUNCTION grest.pool_votes IS 'Get all SPO votes cast for a given pool'; -- noqa: LT01
