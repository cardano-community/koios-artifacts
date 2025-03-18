CREATE OR REPLACE FUNCTION grest.proposal_votes(_proposal_id text)
RETURNS TABLE (
  block_time integer,
  voter_role text,
  voter_id text,
  voter_hex text,
  voter_has_script boolean,
  vote vote,
  meta_url varchar,
  meta_hash text
)
LANGUAGE plpgsql
AS $$
DECLARE
  proposal  text[];
  proposal_id integer;
BEGIN

  SELECT INTO proposal grest.cip129_from_gov_action_id(_proposal_id);

  SELECT gap.id INTO proposal_id
    FROM gov_action_proposal AS gap
      LEFT JOIN tx AS t ON gap.tx_id = t.id
    WHERE t.hash = DECODE(proposal[1], 'hex') AND gap.index = proposal[2]::smallint;

  RETURN QUERY (
    SELECT z.*
    FROM (
      SELECT DISTINCT ON (COALESCE(dh.raw, ph.hash_raw, ch.raw))
        EXTRACT(EPOCH FROM vote_block.time)::integer AS block_time,
        vp.voter_role::text,
        COALESCE(
          grest.cip129_hex_to_drep_id(dh.raw, dh.has_script),
          cardano.bech32_encode('pool', ph.hash_raw),
          grest.cip129_hex_to_cc_hot(ch.raw, ch.has_script)
        ),
        COALESCE(ENCODE(ch.raw, 'hex'), ENCODE(dh.raw, 'hex'), ENCODE(ph.hash_raw, 'hex')) AS voter_hex,
        COALESCE(dh.has_script, ch.has_script, FALSE) AS voter_has_script,
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
      WHERE gap.id = proposal_id
        -- TODO: will we need a similar filters to the one below for pool and committee member retirements?
        AND (
          CASE
            WHEN dh.view IS NOT NULL THEN ((SELECT coalesce(dreg.deposit, 0) FROM drep_registration AS dreg WHERE dreg.drep_hash_id = dh.id ORDER BY id DESC LIMIT 1) >= 0)
            ELSE true
          END)
      ORDER by
        COALESCE(dh.raw, ph.hash_raw, ch.raw),
        block_time DESC
    ) z ORDER BY block_time desc
  );

END;
$$;

COMMENT ON FUNCTION grest.proposal_votes IS 'Get all votes cast on specified governance action'; -- noqa: LT01
