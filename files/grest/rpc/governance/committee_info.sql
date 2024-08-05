CREATE OR REPLACE FUNCTION grest.committee_info()
RETURNS TABLE (
  proposal_tx_hash text,
  cert_index bigint,
  quorum_numerator bigint,
  quorum_denominator bigint,
  members jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  gap_id  bigint;
BEGIN

  SELECT INTO gap_id id
  FROM public.gov_action_proposal
  WHERE type = 'NewCommittee'
    AND enacted_epoch IS NOT NULL
  ORDER BY enacted_epoch DESC
  LIMIT 1;

  RETURN QUERY (
    SELECT
      CASE
        WHEN c.gov_action_proposal_id IS NULL THEN NULL
        ELSE (
          SELECT ENCODE(tx.hash, 'hex')
          FROM gov_action_proposal AS gap
          INNER JOIN tx on gap.tx_id = tx.id
          WHERE gap.id = c.gov_action_proposal_id
        )
      END,
      CASE
        WHEN c.gov_action_proposal_id IS NULL THEN NULL
        ELSE (
          SELECT index
          FROM gov_action_proposal AS gap
          WHERE gap.id = c.gov_action_proposal_id
        )
      END,
      c.quorum_numerator,
      c.quorum_denominator,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'hex', ENCODE(ch.raw, 'hex'),
          'has_script', ch.has_script,
          'expiration_epoch', cm.expiration_epoch
        )
      ) AS members
    FROM public.committee AS c
    INNER JOIN public.committee_member AS cm ON c.id = cm.committee_id
    INNER JOIN public.committee_hash AS ch ON cm.committee_hash_id = ch.id
    WHERE
      CASE
        WHEN gap_id IS NULL THEN c.gov_action_proposal_id IS NULL
        ELSE c.gov_action_proposal_id = gap_id
      END
    GROUP BY c.gov_action_proposal_id, c.quorum_numerator, c.quorum_denominator
  );

END;
$$;

COMMENT ON FUNCTION grest.committee_info IS 'Get information about current governance committee'; --noqa: LT01
