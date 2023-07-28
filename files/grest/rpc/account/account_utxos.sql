CREATE OR REPLACE FUNCTION grest.account_utxos(_stake_address text)
RETURNS TABLE (
  tx_hash text,
  tx_index smallint,
  address varchar,
  value text,
  block_height word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT
    ENCODE(tx.hash,'hex') AS tx_hash,
    tx_out.index::smallint AS tx_index,
    tx_out.address,
    tx_out.value::text AS value,
    b.block_no AS block_height,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time
  FROM
    tx_out
    LEFT JOIN tx_in ON tx_in.tx_out_id = tx_out.tx_id
      AND tx_in.tx_out_index = tx_out.index
    INNER JOIN tx ON tx.id = tx_out.tx_id
    LEFT JOIN block AS b ON b.id = tx.block_id
  WHERE
    tx_in.tx_out_id IS NULL
    AND
    tx_out.stake_address_id = (SELECT id FROM stake_address WHERE view = _stake_address);
END;
$$;

COMMENT ON FUNCTION grest.account_utxos IS 'Get non-empty UTxOs associated with a given stake address'; -- noqa: LT01
