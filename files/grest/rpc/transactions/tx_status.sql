CREATE OR REPLACE FUNCTION grest.tx_status(_tx_hashes text [])
RETURNS TABLE (
  tx_hash text,
  num_confirmations integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    _curr_block_no word31type;
BEGIN
  SELECT
    max(block_no) INTO _curr_block_no
  FROM
    block b;
  RETURN QUERY (
    SELECT
      HASHES,
      (_curr_block_no - b.block_no)
    FROM UNNEST(_tx_hashes) WITH ORDINALITY HASHES
    LEFT OUTER JOIN tx AS t ON t.hash = DECODE(HASHES, 'hex')
    LEFT OUTER JOIN block AS b ON t.block_id = b.id
    ORDER BY ordinality
    );
END;
$$;

COMMENT ON FUNCTION grest.tx_status IS 'Returns number of blocks that were created since the block containing a transactions with a given hash'; -- noqa: LT01
