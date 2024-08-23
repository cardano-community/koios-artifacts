CREATE OR REPLACE FUNCTION grest.block_tx_info(
  _block_hashes text [],
  _inputs boolean DEFAULT false,
  _metadata boolean DEFAULT false,
  _assets boolean DEFAULT false,
  _withdrawals boolean DEFAULT false,
  _certs boolean DEFAULT false,
  _scripts boolean DEFAULT false,
  _bytecode boolean DEFAULT false,
  _governance boolean DEFAULT false
)
RETURNS TABLE (
  tx_hash text,
  block_hash text,
  block_height word31type,
  epoch_no word31type,
  epoch_slot word31type,
  absolute_slot word63type,
  tx_timestamp integer,
  tx_block_index word31type,
  tx_size word31type,
  total_output text,
  fee text,
  treasury_donation text,
  deposit text,
  invalid_before text,
  invalid_after text,
  collateral_inputs jsonb,
  collateral_output jsonb,
  reference_inputs jsonb,
  inputs jsonb,
  outputs jsonb,
  withdrawals jsonb,
  assets_minted jsonb,
  metadata jsonb,
  certificates jsonb,
  native_scripts jsonb,
  plutus_contracts jsonb,
  voting_procedures jsonb,
  proposal_procedures jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  -- all tx ids
  RETURN QUERY
    SELECT *
    FROM grest.tx_info(
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
        ), _inputs, _metadata, _assets, _withdrawals, _certs, _scripts, _bytecode, _governance
      );
END;$$;

COMMENT ON FUNCTION grest.block_tx_info IS 'Get information about transactions for given block hashes.'; -- noqa: LT01
