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
    dd.epoch_no,
    dd.amount::text AS amount
  FROM public.drep_distr AS dd
    INNER JOIN public.drep_hash AS dh ON dh.id = dd.hash_id
  WHERE (CASE WHEN _epoch_no IS NULL THEN TRUE ELSE dd.epoch_no = _epoch_no END)
    AND (CASE
          WHEN _drep_id IS NULL THEN TRUE
          WHEN STARTS_WITH(_drep_id,'drep_always') THEN dh.view = _drep_id
          ELSE dh.raw = DECODE(grest.cip129_drep_id_to_hex(_drep_id), 'hex')
        END)
  ORDER BY dd.epoch_no DESC;
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