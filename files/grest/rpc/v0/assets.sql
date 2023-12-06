-- ASSETS

CREATE OR REPLACE FUNCTION grestv0.asset_address_list(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  payment_address varchar,
  quantity text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_addresses(_asset_policy, _asset_name);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.asset_addresses(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  payment_address varchar,
  quantity text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_addresses(_asset_policy, _asset_name);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.asset_history(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  policy_id text,
  asset_name text,
  fingerprint character varying,
  minting_txs jsonb []
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_policy_decoded bytea;
  _asset_name_decoded bytea;
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_history(_asset_policy, _asset_name);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.asset_info(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  policy_id text,
  asset_name text,
  asset_name_ascii text,
  fingerprint character varying,
  minting_tx_hash text,
  total_supply text,
  mint_cnt bigint,
  burn_cnt bigint,
  creation_time integer,
  minting_tx_metadata jsonb,
  token_registry_metadata jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_info(_asset_policy,_asset_name);

END;
$$;

CREATE OR REPLACE FUNCTION grestv0.asset_info(_asset_list text [] [])
RETURNS TABLE (
  policy_id text,
  asset_name text,
  asset_name_ascii text,
  fingerprint character varying,
  minting_tx_hash text,
  total_supply text,
  mint_cnt bigint,
  burn_cnt bigint,
  creation_time integer,
  minting_tx_metadata jsonb,
  token_registry_metadata jsonb,
  cip68_metadata jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  -- find all asset id's based ON nested array input
  RETURN QUERY
    SELECT * FROM grest.asset_info(_asset_list);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.asset_nft_address(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  payment_address varchar
) LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_nft_address(_asset_policy, _asset_name);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.asset_summary(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  policy_id text,
  asset_name text,
  fingerprint character varying,
  total_transactions bigint,
  staked_wallets bigint,
  unstaked_addresses bigint
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_summary(_asset_policy, _asset_name);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.asset_txs(
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
BEGIN
  RETURN QUERY
    SELECT * FROM grest.asset_txs(_asset_policy, _asset_name, _after_block_height, _history);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.policy_asset_addresses(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  payment_address varchar,
  quantity text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.policy_asset_addresses(_asset_policy);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.asset_policy_info(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  asset_name_ascii text,
  fingerprint varchar,
  minting_tx_hash text,
  total_supply text,
  mint_cnt bigint,
  burn_cnt bigint,
  creation_time integer,
  minting_tx_metadata jsonb,
  token_registry_metadata jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.policy_asset_info(_asset_policy);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.policy_asset_info(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  asset_name_ascii text,
  fingerprint varchar,
  minting_tx_hash text,
  total_supply text,
  mint_cnt bigint,
  burn_cnt bigint,
  creation_time integer,
  minting_tx_metadata jsonb,
  token_registry_metadata jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY 
    SELECT * FROM grest.policy_asset_info(_asset_policy);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.policy_asset_list(_asset_policy text)
RETURNS TABLE (
  asset_name text,
  fingerprint varchar,
  total_supply text,
  decimals integer
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.policy_asset_list(_asset_policy);
END;
$$;
