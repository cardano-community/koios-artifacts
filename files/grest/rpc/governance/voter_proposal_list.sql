CREATE OR REPLACE FUNCTION grest.voter_proposal_list(_voter_id text)
RETURNS TABLE (
  block_time integer,
  proposal_id text,
  proposal_tx_hash text,
  proposal_index bigint,
  proposal_type govactiontype,
  proposal_description jsonb,
  deposit text,
  return_address character varying,
  proposed_epoch word31type,
  ratified_epoch word31type,
  enacted_epoch word31type,
  dropped_epoch word31type,
  expired_epoch word31type,
  expiration word31type,
  meta_url character varying,
  meta_hash text,
  meta_json jsonb,
  meta_comment character varying,
  meta_language character varying,
  meta_is_valid boolean,
  withdrawal jsonb,
  param_proposal jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  _drep_id              bigint;
  _spo_id               bigint;
  _committee_member_id  bigint;
  _gap_id_list          bigint[];
BEGIN

  IF STARTS_WITH(_voter_id, 'drep') THEN
    SELECT INTO _drep_id id FROM public.drep_hash WHERE raw = DECODE((SELECT grest.cip129_drep_id_to_hex(_voter_id)), 'hex');
  ELSIF STARTS_WITH(_voter_id, 'pool') THEN
    SELECT INTO _spo_id id FROM public.pool_hash WHERE view = _voter_id;
  ELSIF STARTS_WITH(_voter_id, 'cc_hot') THEN
    SELECT INTO _committee_member_id id FROM public.committee_hash WHERE raw = DECODE((SELECT grest.cip129_cc_hot_to_hex(_voter_id)), 'hex');
  END IF;

  SELECT INTO _gap_id_list ARRAY_AGG(gov_action_proposal_id)
  FROM (
    SELECT DISTINCT gov_action_proposal_id
    FROM public.voting_procedure
    WHERE
      CASE
        WHEN _drep_id IS NOT NULL THEN drep_voter = _drep_id
        WHEN _spo_id IS NOT NULL THEN pool_voter = _spo_id
        WHEN _committee_member_id IS NOT NULL THEN committee_voter = _committee_member_id
      ELSE
        FALSE
      END
  ) AS tmp;

  RETURN QUERY (
    SELECT
      EXTRACT(EPOCH FROM b.time)::integer,
      grest.cip129_to_gov_action_id(tx.hash, gap.index),
      ENCODE(tx.hash, 'hex'),
      gap.index,
      gap.type,
      gap.description,
      gap.deposit::text,
      sa.view,
      b.epoch_no,
      gap.ratified_epoch,
      gap.enacted_epoch,
      gap.dropped_epoch,
      gap.expired_epoch,
      gap.expiration,
      va.url,
      ENCODE(va.data_hash, 'hex'),
      ocvd.json,
      ocvd.comment,
      ocvd.language,
      ocvd.is_valid,
      CASE
        WHEN tw.id IS NULL THEN NULL
        ELSE
          JSONB_BUILD_OBJECT(
            'stake_address', (
              SELECT sa2.view
              FROM stake_address AS sa2
              WHERE sa2.id = tw.stake_address_id
            ),
            'amount', tw.amount::text
          )
      END AS withdrawal,
      CASE
        WHEN pp.id IS NULL THEN NULL
        ELSE ( SELECT JSONB_STRIP_NULLS(TO_JSONB(pp.*)) - array['id','registered_tx_id','epoch_no'] )
      END AS param_proposal
    FROM public.gov_action_proposal AS gap
      INNER JOIN public.tx ON gap.tx_id = tx.id
      INNER JOIN public.block AS b ON tx.block_id = b.id
      INNER JOIN public.stake_address AS sa ON gap.return_address = sa.id
      LEFT JOIN public.treasury_withdrawal AS tw ON gap.id = tw.gov_action_proposal_id
      LEFT JOIN public.param_proposal AS pp ON gap.param_proposal = pp.id
      LEFT JOIN public.cost_model AS cm ON cm.id = pp.cost_model_id
      LEFT JOIN public.voting_anchor AS va ON gap.voting_anchor_id = va.id
      LEFT JOIN public.off_chain_vote_data AS ocvd ON va.id = ocvd.voting_anchor_id
    WHERE
      gap.id = ANY(_gap_id_list)
    ORDER BY
      b.time DESC
  );

END;
$$;

COMMENT ON FUNCTION grest.voter_proposal_list IS 'Get a raw listing of all governance proposals for specified DRep, SPO or Committee credential'; --noqa: LT01
