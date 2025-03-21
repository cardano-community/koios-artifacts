CREATE OR REPLACE FUNCTION grest.vote_list()
RETURNS TABLE (
  vote_tx_hash text,
  voter_role text,
  voter_id text,
  proposal_id text,
  proposal_tx_hash text,
  proposal_index bigint,
  proposal_type text,
  epoch_no word31type,
  block_height word31type,
  block_time integer,
  vote text,
  meta_url text,
  meta_hash text,
  meta_json jsonb
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(vote_tx.hash, 'hex'),
    vp.voter_role::text,
    COALESCE(
      grest.cip129_hex_to_drep_id(dh.raw, dh.has_script),
      cardano.bech32_encode('pool', ph.hash_raw),
      grest.cip129_hex_to_cc_hot(ch.raw, ch.has_script)
    ),
    grest.cip129_to_gov_action_id(prop_tx.hash, gap.index),
    ENCODE(prop_tx.hash, 'hex'),
    gap.index::bigint,
    gap.type::text,
    b.epoch_no,
    b.block_no,
    EXTRACT(EPOCH FROM b.time)::integer,
    vp.vote,
    va.url,
    ENCODE(va.data_hash, 'hex'),
    ocvd.json
  FROM public.voting_procedure AS vp
    INNER JOIN public.tx AS vote_tx on vp.tx_id = vote_tx.id
    INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
    INNER JOIN public.tx AS prop_tx ON prop_tx.id = gap.tx_id
    INNER JOIN public.block AS b ON vote_tx.block_id = b.id
    LEFT JOIN public.drep_hash AS dh ON vp.drep_voter = dh.id
    LEFT JOIN public.pool_hash AS ph ON vp.pool_voter = ph.id
    LEFT JOIN public.committee_hash AS ch ON vp.committee_voter = ch.id
    LEFT JOIN public.voting_anchor AS va ON vp.voting_anchor_id = va.id
    LEFT JOIN public.off_chain_vote_data AS ocvd ON ocvd.voting_anchor_id = va.id
  ORDER BY
    vp.tx_id DESC;
$$;
COMMENT ON FUNCTION grest.vote_list IS 'Get a listing of all votes posted on-chain'; --noqa: LT01
