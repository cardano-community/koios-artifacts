CREATE OR REPLACE FUNCTION grest.tx_cbor(_tx_hashes text [])
RETURNS TABLE (
  tx_hash text,
  block_hash text,
  block_height word31type,
  epoch_no word31type,
  absolute_slot word63type,
  tx_timestamp integer,
  cbor text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(tx.hash::bytea, 'hex'),
    ENCODE(block.hash, 'hex'),
    block.block_no,
    block.epoch_no,
    block.slot_no,
    EXTRACT(EPOCH FROM block.time)::integer,
    ENCODE(tx_cbor.bytes::bytea, 'hex')
  FROM public.tx
    INNER JOIN block ON block.id = tx.block_id
    LEFT JOIN public.tx_cbor ON tx.id = tx_cbor.tx_id
  WHERE tx.hash::bytea = ANY(
      SELECT
        DECODE(hashes, 'hex')
      FROM
        UNNEST(_tx_hashes) AS hashes
    )
  ORDER BY tx.id;
$$;

COMMENT ON FUNCTION grest.tx_cbor IS 'Get raw transaction(s) in CBOR format'; -- noqa: LT01

CREATE OR REPLACE FUNCTION grest.block_tx_cbor(_block_hashes text [])
RETURNS TABLE (
  tx_hash text,
  block_hash text,
  block_height word31type,
  epoch_no word31type,
  absolute_slot word63type,
  tx_timestamp integer,
  cbor text
)
LANGUAGE sql STABLE
AS $$
  SELECT *
  FROM grest.tx_cbor(
      (
        SELECT ARRAY_AGG(ENCODE(hash, 'hex'))
        FROM  tx
        WHERE tx.block_id = ANY(
          SELECT id
          FROM block
          WHERE hash = ANY(
            SELECT DISTINCT DECODE(hashes_hex, 'hex')
            FROM UNNEST(_block_hashes) AS hashes_hex
          )
        )
      )
    );
$$;

COMMENT ON FUNCTION grest.block_tx_cbor IS 'Get Raw transaction in CBOR format for given block hashes.'; -- noqa: LT01
