CREATE OR REPLACE FUNCTION grest.asset_list()
RETURNS TABLE (
  policy_id text,
  asset_name text,
  asset_name_ascii text,
  fingerprint text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(ma.policy, 'hex')::text AS policy_id,
    ENCODE(ma.name, 'hex')::text AS asset_name,
    ENCODE(ma.name, 'escape')::text as asset_name_ascii,
    ma.fingerprint::text
  FROM public.multi_asset AS ma
  ORDER BY ma.policy, ma.name;
$$;

COMMENT ON FUNCTION grest.asset_list IS 'Get a raw listing of all native assets on chain, without any CIP overlays';
