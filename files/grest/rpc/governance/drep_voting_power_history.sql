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
    COALESCE(dd.epoch_no, e.no) as epoch_no,
    COALESCE(dd.amount::text, '0') AS amount
  FROM public.drep_hash AS dh
  INNER JOIN public.epoch AS e on e.no > (
    CASE 
      WHEN dh.raw IS NULL THEN (SELECT MIN(epoch_no) - 1 FROM drep_distr AS predef_dd WHERE predef_dd.hash_id = dh.id)
      ELSE (
        -- earliest voting power can be recorded for epoch after very first registration's epoch
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
  -- previously was doing INNER JOIN of drep_distr with drep_hash but if zero voting power drep_distr not created
  LEFT OUTER JOIN public.drep_distr AS dd on dh.id = dd.hash_id AND e.no = dd.epoch_no
  WHERE (CASE
        WHEN _epoch_no IS NULL
        THEN TRUE ELSE dd.epoch_no = _epoch_no
      END)
    AND (CASE
          WHEN _drep_id IS NULL THEN TRUE
          WHEN STARTS_WITH(_drep_id,'drep_always') THEN dh.view = _drep_id
          ELSE dh.raw = DECODE(grest.cip129_drep_id_to_hex(_drep_id), 'hex')
        END)
  ORDER BY COALESCE(dd.epoch_no, e.no) DESC;
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
