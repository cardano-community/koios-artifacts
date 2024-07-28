CREATE OR REPLACE FUNCTION grest.drep_info(_drep_ids text [])
RETURNS TABLE (
  drep_id character varying,
  hex text,
  has_script boolean,
  registered boolean,
  deposit text,
  active boolean,
  amount text
)
LANGUAGE plpgsql
AS $$
DECLARE
  curr_epoch    word31type;
BEGIN

  SELECT INTO curr_epoch MAX(epoch_no) FROM public.block;

  RETURN QUERY
  SELECT
    DISTINCT ON (dh.view) dh.view AS drep_id,
    ENCODE(dh.raw, 'hex')::text AS hex,
    dh.has_script AS has_script,
    (CASE WHEN (dr.deposit IS NOT NULL AND dr.deposit >= 0) OR starts_with(dh.view,'drep_') THEN TRUE ELSE FALSE END) AS registered,
    dr.deposit::text AS deposit,
    (CASE WHEN (dd.active_until IS NOT NULL AND dd.active_until > curr_epoch) OR starts_with(dh.view,'drep_') THEN TRUE ELSE FALSE END) AS active,
    COALESCE(dd.amount, 0)::text AS amount
  FROM public.drep_hash dh
    LEFT JOIN public.drep_registration dr ON dh.id = dr.drep_hash_id
    LEFT JOIN public.drep_distr dd ON dh.id = dd.hash_id AND dd.epoch_no = curr_epoch
  WHERE dh.view = ANY(_drep_ids)
  ORDER BY
    dh.view, dr.tx_id DESC;

END;
$$;

COMMENT ON FUNCTION grest.drep_info IS 'Get bulk DRep info from bech32 formatted DRep IDs, incl predefined roles ''drep_always_abstain'' and ''drep_always_no_confidence'''; -- noqa: LT01
