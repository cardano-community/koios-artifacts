-- BLOCKS

CREATE OR REPLACE FUNCTION grestv0.block_info(_block_hashes text [])
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
BEGIN
  RETURN QUERY
  SELECT * FROM grest.block_info(_block_hashes);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.block_txs(_block_hashes text [])
RETURNS TABLE (
  block_hash text,
  tx_hashes text []
)
LANGUAGE plpgsql
AS $$
DECLARE
  _block_hashes_bytea bytea[];
  _block_ids integer[];
BEGIN
  SELECT INTO _block_hashes_bytea ARRAY_AGG(block_hashes_bytea)
  FROM (
    SELECT DECODE(hex, 'hex') AS block_hashes_bytea
    FROM UNNEST(_block_hashes) AS hex
  ) AS tmp;

  SELECT INTO _block_ids ARRAY_AGG(b.id)
  FROM public.block AS b
  WHERE b.hash = ANY(_block_hashes_bytea);

  RETURN QUERY
    SELECT
      encode(b.hash, 'hex'),
      ARRAY_AGG(ENCODE(tx.hash::bytea, 'hex'))
    FROM
      public.block AS b
      INNER JOIN public.tx ON tx.block_id = b.id
    WHERE b.id = ANY(_block_ids)
    GROUP BY b.hash;
END;
$$;
