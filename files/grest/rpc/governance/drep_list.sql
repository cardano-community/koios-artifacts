CREATE OR REPLACE FUNCTION grest.drep_list()
RETURNS TABLE (
  drep_id character varying,
  hex text,
  has_script boolean,
  registered boolean
)
LANGUAGE sql STABLE
AS $$
  SELECT
    DISTINCT ON (dh.view) dh.view AS drep_id,
    ENCODE(dh.raw, 'hex')::text AS hex,
    dh.has_script AS has_script,
    (CASE
      WHEN coalesce(dr.deposit, 0) >= 0 THEN TRUE
      ELSE FALSE
    END) AS registered
  FROM public.drep_hash AS dh
    INNER JOIN public.drep_registration AS dr ON dh.id = dr.drep_hash_id
  ORDER BY
    dh.view, dr.tx_id DESC;
$$;

COMMENT ON FUNCTION grest.asset_list IS 'Get a raw listing of all active delegated representatives, aka DReps'; --noqa: LT01
