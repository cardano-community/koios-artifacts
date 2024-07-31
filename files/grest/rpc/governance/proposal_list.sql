CREATE OR REPLACE FUNCTION grest.proposal_list()
RETURNS TABLE (
  tx_hash text,
  cert_index integer,
  block_time integer,
  proposal_type text,
  proposal_description jsonb,
  deposit text,
  return_address text,
  proposed_epoch integer,
  ratified_epoch integer,
  enacted_epoch integer,
  dropped_epoch integer,
  expired_epoch integer,
  expiration integer,
  meta_url text,
  meta_hash text,
  meta_json jsonb,
  meta_comment text,
  meta_language text,
  meta_is_valid boolean,
  withdrawal jsonb,
  param_proposal jsonb
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(tx.hash, 'hex')::text AS tx_hash,
    gap.index AS cert_index,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    gap.type AS proposal_type,
    gap.description AS proposal_description,
    gap.deposit::text AS deposit,
    sa.view AS return_address,
    b.epoch_no AS proposed_epoch,
    gap.ratified_epoch AS ratified_epoch,
    gap.enacted_epoch AS enacted_epoch,
    gap.dropped_epoch AS dropped_epoch,
    gap.expired_epoch AS expired_epoch,
    gap.expiration AS expiration,
    va.url AS meta_url,
    ENCODE(va.data_hash, 'hex') AS meta_hash,
    ocvd.json AS meta_json,
    ocvd.comment AS meta_comment,
    ocvd.language AS meta_language,
    ocvd.is_valid AS meta_is_valid,
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
      ELSE ( SELECT ROW_TO_JSON(pp.*) )
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
  ORDER BY
    block_time DESC;
$$;

COMMENT ON FUNCTION grest.proposal_list IS 'Get a raw listing of all governance proposals'; --noqa: LT01
