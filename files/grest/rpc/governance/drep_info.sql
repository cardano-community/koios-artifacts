CREATE OR REPLACE FUNCTION grest.drep_info(_drep_ids text [])
RETURNS TABLE (
  drep_id text,
  hex text,
  has_script boolean,
  registered boolean,
  deposit text,
  active boolean,
  expires_epoch_no numeric,
  amount text,
  url character varying,
  hash text
)
LANGUAGE plpgsql
AS $$
DECLARE
  curr_epoch    word31type;
  drep_ids_raw  hash28type[];
  drep_list     bigint[];
  drep_activity word64type;
BEGIN

  SELECT INTO curr_epoch MAX(epoch_no) FROM public.block;

  SELECT INTO drep_activity ep.drep_activity FROM public.epoch_param AS ep WHERE ep.epoch_no = curr_epoch;

  SELECT INTO drep_ids_raw ARRAY_REMOVE(ARRAY_AGG(
    CASE
      WHEN STARTS_WITH(n,'drep_') THEN NULL
    ELSE
      DECODE(grest.cip129_drep_id_to_hex(n), 'hex')
    END
  ), NULL) FROM UNNEST(_drep_ids) AS n;

  -- all DRep ids
  SELECT INTO drep_list ARRAY_AGG(id)
  FROM (
    SELECT id
    FROM public.drep_hash
    WHERE raw = ANY(drep_ids_raw)
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
          (CASE WHEN (curr_epoch - b.epoch_no) < drep_activity THEN TRUE ELSE FALSE END) AS active,
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
      )

    SELECT DISTINCT ON (dh.view)
      CASE
        WHEN dh.raw IS NULL THEN dh.view
      ELSE
        grest.cip129_hex_to_drep_id(dh.raw, dh.has_script)
      END AS drep_id,
      ENCODE(dh.raw, 'hex')::text AS hex,
      dh.has_script AS has_script,
      (CASE WHEN starts_with(dh.view,'drep_') OR (COALESCE(dr.deposit, 0) >= 0 AND dr.drep_hash_id IS NOT NULL) THEN TRUE ELSE FALSE END) AS registered,
      (CASE WHEN (dr.deposit < 0) OR starts_with(dh.view,'drep_') THEN NULL ELSE ds.deposit END)::text AS deposit,
      (CASE WHEN starts_with(dh.view,'drep_') THEN TRUE ELSE COALESCE(dr.deposit, 0) >= 0 AND ds.active END) AS active,
      (CASE WHEN COALESCE(dr.deposit, 0) >= 0 THEN ds.expires_epoch_no ELSE NULL END) AS expires_epoch_no,
      COALESCE(dd.amount, 0)::text AS amount,
      va.url,
      ENCODE(va.data_hash, 'hex') AS hash
    FROM public.drep_hash AS dh
      LEFT JOIN public.drep_registration AS dr ON dh.id = dr.drep_hash_id
      LEFT JOIN public.voting_anchor AS va ON dr.voting_anchor_id = va.id
      LEFT JOIN public.drep_distr AS dd ON dh.id = dd.hash_id AND dd.epoch_no = curr_epoch
      LEFT JOIN _drep_state AS ds ON dh.id = ds.drep
    WHERE dh.id = ANY(drep_list)
    ORDER BY
      dh.view, dr.tx_id DESC
  );

END;
$$;

COMMENT ON FUNCTION grest.drep_info IS 'Get bulk DRep info from bech32 formatted DRep IDs (CIP-5 | CIP-129), incl predefined roles ''drep_always_abstain'' and ''drep_always_no_confidence'''; -- noqa: LT01
