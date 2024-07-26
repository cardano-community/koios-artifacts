CREATE OR REPLACE FUNCTION grest.drep_epoch_summary(_epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  epoch_no word31type,
  amount text,
  dreps integer
)
LANGUAGE sql STABLE
AS $$
  SELECT
    epoch_no,
    SUM(amount)::text AS amount,
    COUNT(hash_id)
  FROM public.drep_distr
  WHERE (CASE WHEN _epoch_no IS NULL THEN TRUE ELSE epoch_no = _epoch_no END)
  GROUP BY epoch_no
  ORDER BY epoch_no DESC;
$$;

COMMENT ON FUNCTION grest.drep_epoch_summary IS 'Get a summary of vote power and active DReps by specified epoch or all'; --noqa: LT01
