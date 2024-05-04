CREATE OR REPLACE FUNCTION grest.asset_txs(
  _asset_policy text,
  _asset_name text DEFAULT '',
  _after_block_height integer DEFAULT 0,
  _history boolean DEFAULT FALSE
)
RETURNS TABLE (
  tx_hash text,
  epoch_no word31type,
  block_height word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_policy_decoded bytea;
  _asset_name_decoded bytea;
  _asset_id int;
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  SELECT DECODE(
    CASE
      WHEN _asset_name IS NULL THEN ''
      ELSE _asset_name
    END,
    'hex'
  ) INTO _asset_name_decoded;
  SELECT id INTO _asset_id
  FROM multi_asset AS ma
  WHERE ma.policy = _asset_policy_decoded AND ma.name = _asset_name_decoded;

  RETURN QUERY
    SELECT
      ENCODE(tx_hashes.hash, 'hex') AS tx_hash,
      tx_hashes.epoch_no,
      tx_hashes.block_no,
      EXTRACT(EPOCH FROM tx_hashes.time)::integer
    FROM (
      SELECT DISTINCT ON (tx.hash)
        tx.hash,
        block.epoch_no,
        block.block_no,
        block.time
      FROM ma_tx_out AS mto
      LEFT JOIN ma_tx_mint AS mtm ON mto.ident = mtm.ident
      INNER JOIN tx_out AS txo ON txo.id = mto.tx_out_id
      INNER JOIN tx ON tx.id = txo.tx_id OR tx.id = mtm.tx_id
      INNER JOIN block ON block.id = tx.block_id
      WHERE
        mto.ident = _asset_id
        AND block.block_no >= _after_block_height
        AND (_history = TRUE OR txo.consumed_by_tx_id IS NULL)
      GROUP BY
        mto.ident,
        tx.hash,
        txo.index::smallint,
        block.epoch_no,
        block.block_no,
        block.time
    ) AS tx_hashes ORDER BY tx_hashes.block_no DESC;
END;
$$;

COMMENT ON FUNCTION grest.asset_txs IS 'Get the list of all asset transaction hashes (newest first)'; -- noqa: LT01
