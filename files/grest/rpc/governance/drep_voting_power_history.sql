CREATE OR REPLACE FUNCTION grest.drep_voting_power_history(_epoch_no numeric DEFAULT NULL, _drep_id text DEFAULT NULL)
RETURNS TABLE (
  drep_id text,
  epoch_no word31type,
  amount text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    CASE
      WHEN dh.raw IS NULL THEN dh.view
      ELSE grest.cip129_hex_to_drep_id(dh.raw, dh.has_script)
    END AS drep_id,
    COALESCE(dd.epoch_no, e.epoch_no) as epoch_no,
    COALESCE(dd.amount::text, '0') AS amount
  FROM public.drep_hash AS dh
  -- Use grest.epoch_info_cache (small fixed-size table) instead of public.epoch view
  -- which has a ~1.2s overhead from the epoch_current aggregate. The cache is maintained
  -- by grest.epoch_info_cache_update() and is complete in steady state; if it is
  -- sparse (e.g. immediately after a cache reset), this query will miss those epochs
  -- until the next cache update completes.
  INNER JOIN grest.epoch_info_cache AS e on e.epoch_no > (
    CASE
      WHEN dh.raw IS NULL THEN COALESCE(
        (SELECT MIN(epoch_no) - 1 FROM drep_distr AS predef_dd WHERE predef_dd.hash_id = dh.id),
        (SELECT MIN(epoch_no) FROM grest.epoch_info_cache) - 1
      )
      ELSE (
        SELECT b.epoch_no
        FROM drep_registration AS dr
        INNER JOIN tx AS t ON dr.drep_hash_id = dh.id AND dr.tx_id = t.id
        INNER JOIN block AS b ON t.block_id = b.id
        WHERE NOT EXISTS (
          SELECT 1
          FROM drep_registration AS dr2
          WHERE dr2.drep_hash_id = dr.drep_hash_id AND dr2.id < dr.id
        )
      )
    END
  )
  AND e.epoch_no <= (
    CASE
      WHEN ((SELECT deposit FROM drep_registration dr WHERE dr.drep_hash_id = dh.id ORDER BY id DESC LIMIT 1) < 0)
      THEN (SELECT b.epoch_no FROM block b INNER JOIN tx t ON b.id = t.block_id and t.id =
        (SELECT tx_id FROM drep_registration dr WHERE dr.drep_hash_id = dh.id ORDER BY id DESC LIMIT 1))
      ELSE (SELECT MAX(epoch_param.epoch_no) FROM public.epoch_param)
    END
  )
  -- previously was doing INNER JOIN of drep_distr with drep_hash but if zero voting power drep_distr not created
  LEFT OUTER JOIN public.drep_distr AS dd on dh.id = dd.hash_id AND e.epoch_no = dd.epoch_no
  WHERE (CASE
        WHEN _epoch_no IS NULL
        THEN TRUE ELSE dd.epoch_no = _epoch_no::word31type
      END)
    AND (CASE
          WHEN _drep_id IS NULL THEN TRUE
          WHEN STARTS_WITH(_drep_id,'drep_always') THEN dh.view = _drep_id
          ELSE dh.raw = DECODE(grest.cip129_drep_id_to_hex(_drep_id), 'hex')
        END)
  ORDER BY COALESCE(dd.epoch_no, e.epoch_no) DESC;
$$;

COMMENT ON FUNCTION grest.drep_voting_power_history IS 'Get history for dreps voting power distribution'; --noqa: LT01

CREATE OR REPLACE FUNCTION grest.drep_history(_epoch_no numeric DEFAULT NULL, _drep_id text DEFAULT NULL)
RETURNS TABLE (
  drep_id text,
  epoch_no word31type,
  amount text
)
LANGUAGE sql STABLE
AS $$
  SELECT *
  FROM grest.drep_voting_power_history(_epoch_no, _drep_id);
$$;
