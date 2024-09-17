CREATE OR REPLACE FUNCTION grest.cli_protocol_params()
RETURNS JSON
LANGUAGE sql STABLE
AS $$
  SELECT ct.artifacts::json
  FROM grest.control_table AS ct
  WHERE ct.key = 'cli_protocol_params';
$$;
