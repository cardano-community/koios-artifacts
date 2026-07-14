CREATE OR REPLACE FUNCTION grest.drep_info(_drep_ids text [])
RETURNS TABLE (
  drep_id text,
  hex text,
  has_script boolean,
  drep_status text,
  deposit text,
  active boolean,
  expires_epoch_no numeric,
  amount text,
  meta_url varchar,
  meta_hash text,
  live_delegator_count bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
  curr_epoch    word31type;
  drep_list     bigint[];
  drep_activity word64type;
BEGIN

  SELECT INTO curr_epoch MAX(epoch_param.epoch_no) FROM public.epoch_param;

  SELECT INTO drep_activity ep.drep_activity FROM public.epoch_param AS ep WHERE ep.epoch_no = curr_epoch;

  -- all DRep ids
  SELECT INTO drep_list ARRAY_AGG(id)
  FROM (
    SELECT id
    FROM public.drep_hash AS dh
    INNER JOIN (
      SELECT
        CASE
          WHEN STARTS_WITH(n,'drep_always') THEN NULL
        ELSE
          DECODE(grest.cip129_drep_id_to_hex(n), 'hex')
        END AS hex,
        grest.cip129_drep_id_has_script(n) AS has_script
      FROM UNNEST(_drep_ids) AS n
    ) AS dip ON dip.hex = dh.raw AND dip.has_script = dh.has_script
    WHERE dh.raw IS NOT NULL
  ) AS tmp;

  IF 'drep_always_abstain' = ANY(_drep_ids) THEN
    SELECT INTO drep_list ARRAY_APPEND(drep_list, id)
    FROM public.drep_hash
    WHERE view = 'drep_always_abstain';
  END IF;

  IF 'drep_always_no_confidence' = ANY(_drep_ids) THEN
    SELECT INTO drep_list ARRAY_APPEND(drep_list, id)
    FROM public.drep_hash
    WHERE view = 'drep_always_no_confidence';
  END IF;

  RETURN QUERY (
    WITH

      _last_registration AS (
        SELECT
          DISTINCT ON (dr.drep_hash_id) drep_hash_id AS drep,
          dr.tx_id,
          dr.deposit
        FROM
          public.drep_registration AS dr
        WHERE
          dr.drep_hash_id = ANY(drep_list)
          AND (dr.deposit IS NOT NULL AND dr.deposit >= 0)
        ORDER BY
          dr.drep_hash_id, dr.tx_id DESC
      ),

      _last_update AS (
        SELECT
          dr.drep_hash_id AS drep,
          MAX(dr.tx_id) AS tx_id
        FROM
          public.drep_registration AS dr
        WHERE
          dr.drep_hash_id = ANY(drep_list)
          AND dr.deposit IS NULL
        GROUP BY dr.drep_hash_id
      ),

      _last_vote AS (
        SELECT
          drep_voter AS drep,
          MAX(tx_id) AS tx_id
        FROM
          public.voting_procedure
        WHERE
          drep_voter = ANY(drep_list)
        GROUP BY drep_voter
      ),

      _drep_state AS (
        SELECT
          drep_state.drep,
          drep_state.deposit,
          (CASE WHEN (curr_epoch - b.epoch_no) <= drep_activity THEN TRUE ELSE FALSE END) AS active,
          (b.epoch_no + drep_activity) AS expires_epoch_no
        FROM (
          SELECT
            all_activities.drep,
            MAX(all_activities.tx_id) AS last_tx_id,
            MAX(all_activities.deposit) AS deposit
          FROM (
            SELECT lr.drep, lr.tx_id, lr.deposit FROM _last_registration AS lr
            UNION ALL
            SELECT lu.drep, lu.tx_id, 0 AS deposit FROM _last_update AS lu
            UNION ALL
            SELECT lv.drep, lv.tx_id, 0 AS deposit FROM _last_vote AS lv
          ) AS all_activities
          GROUP BY all_activities.drep
        ) AS drep_state
        INNER JOIN tx on drep_state.last_tx_id = tx.id
        INNER JOIN block AS b ON tx.block_id = b.id
      ),

      _latest_deleg AS ( -- global latest delegation per addr, computed once
      SELECT DISTINCT ON (dv.addr_id)
        dv.addr_id, dv.tx_id, dv.drep_hash_id
      FROM public.delegation_vote AS dv
      ORDER BY dv.addr_id, dv.tx_id DESC
      ),

      _all_delegations AS (
        SELECT latest.addr_id, latest.tx_id, latest.drep_hash_id
        FROM _latest_deleg AS latest
        -- only interested in delegations since last registration for normal
        -- dreps, and all latest delegations for predefined ones
        LEFT JOIN _last_registration lr ON lr.drep = latest.drep_hash_id
        WHERE latest.drep_hash_id = ANY(drep_list)
          AND (lr.tx_id IS NULL OR latest.tx_id >= lr.tx_id)
          AND NOT EXISTS (
            SELECT 1 FROM public.stake_deregistration sd
            WHERE sd.addr_id = latest.addr_id AND sd.tx_id > latest.tx_id
          )
      ),

      _deleg_counts AS ( -- one row per drep, computed once
        SELECT drep_hash_id, count(*) AS live_delegator_count
        FROM _all_delegations
        GROUP BY drep_hash_id
      )

    SELECT DISTINCT ON (dh.view)
      CASE
        WHEN dh.raw IS NULL THEN dh.view
      ELSE
        grest.cip129_hex_to_drep_id(dh.raw, dh.has_script)
      END AS drep_id,
      ENCODE(dh.raw, 'hex')::text AS hex,
      dh.has_script AS has_script,
      (CASE
        WHEN starts_with(dh.view,'drep_always') THEN 'registered'
        WHEN dr.drep_hash_id IS NULL THEN 'not_registered'
        WHEN dr.deposit < 0 THEN 'deregistered'
        ELSE 'registered'
      END) AS drep_status,
      (CASE WHEN (dr.deposit < 0) OR starts_with(dh.view,'drep_always') THEN NULL ELSE ds.deposit END)::text AS deposit,
      COALESCE(starts_with(dh.view,'drep_always') OR (COALESCE(dr.deposit, 0) >= 0 AND (ds.active OR COALESCE(dd.active_until, 0) > curr_epoch)), FALSE) AS active,
      (CASE WHEN COALESCE(dr.deposit, 0) >= 0 THEN GREATEST(ds.expires_epoch_no, COALESCE(dd.active_until, 0)) ELSE NULL END) AS expires_epoch_no,
      COALESCE(dd.amount, 0)::text AS amount,
      va.url AS meta_url,
      ENCODE(va.data_hash, 'hex') AS meta_hash,
      COALESCE(dc.live_delegator_count, 0) AS live_delegator_count
    FROM public.drep_hash AS dh
      LEFT JOIN public.drep_registration AS dr ON dh.id = dr.drep_hash_id
      LEFT JOIN public.voting_anchor AS va ON dr.voting_anchor_id = va.id
      LEFT JOIN public.drep_distr AS dd ON dh.id = dd.hash_id AND dd.epoch_no = curr_epoch
      LEFT JOIN _drep_state AS ds ON dh.id = ds.drep
      LEFT JOIN _deleg_counts AS dc ON dc.drep_hash_id = dh.id
    WHERE dh.id = ANY(drep_list)
    ORDER BY
      dh.view, dr.tx_id DESC
  );

END;
$$;

COMMENT ON FUNCTION grest.drep_info IS 'Get bulk DRep info from bech32 formatted DRep IDs (CIP-5 | CIP-129), incl predefined roles ''drep_always_abstain'' and ''drep_always_no_confidence'''; -- noqa: LT01
