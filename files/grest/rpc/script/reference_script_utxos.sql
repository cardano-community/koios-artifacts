CREATE OR REPLACE FUNCTION grest.reference_script_utxos(_script_hashes text[])
RETURNS TABLE (
  script_hash text,
  tx_hash text,
  tx_index smallint
)
LANGUAGE plpgsql
AS $$
DECLARE
  _script_hashes_bytea  bytea[];
BEGIN
  -- convert input _script_hashes array into bytea array
  SELECT INTO _script_hashes_bytea ARRAY_AGG(hashes_bytea)
  FROM (
    SELECT DECODE(hashes_hex, 'hex') AS hashes_bytea
    FROM UNNEST(_script_hashes) AS hashes_hex
  ) AS tmp;

  RETURN QUERY (
    SELECT
      ENCODE(script.hash, 'hex'),
      ENCODE(tx.hash, 'hex'),
      tx_out.index::smallint
    FROM script
    INNER JOIN tx_out ON tx_out.reference_script_id = script.id
    INNER JOIN tx ON tx.id = tx_out.tx_id
    WHERE script.hash = ANY(_script_hashes_bytea)
      AND tx_out.consumed_by_tx_id IS NULL
  );

END;
$$;

COMMENT ON FUNCTION grest.reference_script_utxos IS 'Get all unspent utxos with a reference script matching given script hashes.'; -- noqa: LT01
