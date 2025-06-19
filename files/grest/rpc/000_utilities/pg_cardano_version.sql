CREATE OR REPLACE FUNCTION grest.pg_cardano_version()
RETURNS TABLE (
  extversion text
)
LANGUAGE sql STABLE
AS $$
  SELECT extversion FROM pg_extension
  WHERE extname = 'pg_cardano';
$$;

COMMENT ON FUNCTION grest.pg_cardano_version IS 'Returns the version of pg_cardano installed';
