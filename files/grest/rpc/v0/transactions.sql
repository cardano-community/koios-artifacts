-- TX

CREATE OR REPLACE FUNCTION grestv0.tx_info(_tx_hashes text [])
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
  plutus_contracts jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM tx_info(_tx_hashes);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.tx_metadata(_tx_hashes text [])
RETURNS TABLE (
  tx_hash text,
  metadata jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT * FROM grest.tx_metadata(_tx_hashes);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.tx_metalabels()
RETURNS TABLE (key text)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT * FROM grest.tx_metalabels();
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.tx_status(_tx_hashes text [])
RETURNS TABLE (
  tx_hash text,
  num_confirmations integer
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.tx_status(_tx_hashes);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.tx_utxos(_tx_hashes text [])
RETURNS TABLE (
  tx_hash text,
  inputs jsonb,
  outputs jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.tx_utxos(_tx_hashes);
END;
$$;
