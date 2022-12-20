CREATE OR REPLACE FUNCTION grest.asset_info (_asset_policy text, _asset_name text default '')
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
    token_registry_metadata json
  )
  LANGUAGE PLPGSQL
  AS $$
BEGIN
  
  RETURN QUERY
    SELECT grest.asset_info_bulk(array[array[_asset_policy, _asset_name]]);
      
END;
$$;

COMMENT ON FUNCTION grest.asset_info IS 'Get the information of an asset incl first minting & token registry metadata';

