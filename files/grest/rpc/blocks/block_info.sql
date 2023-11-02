CREATE OR REPLACE FUNCTION grest.block_info(_block_hashes text [])
RETURNS TABLE (
  hash text,
  epoch_no word31type,
  abs_slot word63type,
  epoch_slot word31type,
  block_height word31type,
  block_size word31type,
  block_time integer,
  tx_count bigint,
  vrf_key varchar,
  op_cert text,
  op_cert_counter word63type,
  pool varchar,
  proto_major word31type,
  proto_minor word31type,
  total_output text,
  total_fees text,
  num_confirmations integer,
  parent_hash text,
  child_hash text
)
LANGUAGE plpgsql
AS $$
DECLARE
  _block_hashes_bytea   bytea[];
  _block_id_list        bigint[];
  _curr_block_no        word31type;
BEGIN
  SELECT MAX(block_no) INTO _curr_block_no
  FROM block AS b;

  -- convert input _block_hashes array into bytea array
  SELECT INTO _block_hashes_bytea ARRAY_AGG(hashes_bytea)
  FROM (
    SELECT DECODE(hashes_hex, 'hex') AS hashes_bytea
    FROM
      UNNEST(_block_hashes) AS hashes_hex
  ) AS tmp;

  -- all block ids
  SELECT INTO _block_id_list ARRAY_AGG(id)
  FROM (
    SELECT id
    FROM block
    WHERE block.hash = ANY(_block_hashes_bytea)
  ) AS tmp;

  RETURN QUERY
  SELECT
    ENCODE(b.hash, 'hex') AS hash,
    b.epoch_no AS epoch,
    b.slot_no AS abs_slot,
    b.epoch_slot_no AS epoch_slot,
    b.block_no AS block_height,
    b.size AS block_size,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    b.tx_count,
    b.vrf_key,
    ENCODE(b.op_cert::bytea, 'hex') AS op_cert,
    b.op_cert_counter,
    ph.view AS pool,
    b.proto_major,
    b.proto_minor,
    block_data.total_output::text,
    block_data.total_fees::text,
    (_curr_block_no - b.block_no) AS num_confirmations,
    (
      SELECT ENCODE(tb.hash::bytea, 'hex')
      FROM block tb
      WHERE id = b.previous_id
    ) AS parent_hash,
    (
      SELECT ENCODE(tb.hash::bytea, 'hex')
      FROM block tb
      WHERE previous_id = b.id
    ) AS child_hash
  FROM
    block AS b
    LEFT JOIN slot_leader AS sl ON sl.id = b.slot_leader_id
    LEFT JOIN pool_hash AS ph ON ph.id = sl.pool_hash_id
    LEFT JOIN LATERAL (
      SELECT
        SUM(tx_data.total_output) AS total_output,
        SUM(tx.fee) AS total_fees
      FROM
        tx
        JOIN LATERAL (
          SELECT SUM(tx_out.value) AS total_output
          FROM tx_out
          WHERE tx_out.tx_id = tx.id
        ) tx_data ON TRUE
      WHERE
        tx.block_id = b.id
    ) block_data ON TRUE
  WHERE
    b.id = ANY(_block_id_list);
END;
$$;

COMMENT ON FUNCTION grest.block_info IS 'Get detailed information about list of block hashes'; --noqa: LT01
