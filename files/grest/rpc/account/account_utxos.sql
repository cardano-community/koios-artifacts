CREATE OR REPLACE FUNCTION grest.account_utxos (_stake_address text)
  RETURNS TABLE (
    tx_hash text,
    tx_index smallint,
    address varchar,
    value text,
    block_height word31type,
    block_time integer
  )
  LANGUAGE PLPGSQL
  AS $$

BEGIN
  RETURN QUERY
  SELECT
    ENCODE(tx.hash,'hex') as tx_hash,
    tx_out.index::smallint as tx_index,
    tx_out.address,
    tx_out.value::text as value,
    b.block_no as block_height,
    EXTRACT(epoch from b.time)::integer as block_time
  FROM
    tx_out
    LEFT JOIN tx_in ON tx_in.tx_out_id = tx_out.tx_id
      AND tx_in.tx_out_index = tx_out.index
    INNER JOIN tx ON tx.id = tx_out.tx_id
    LEFT JOIN block b ON b.id = tx.block_id
  WHERE
    tx_in.tx_out_id IS NULL
    AND
    tx_out.stake_address_id = (select id from stake_address where view = _stake_address);

END;
$$;

COMMENT ON FUNCTION grest.account_utxos IS 'Get non-empty UTxOs associated with a given stake address';
