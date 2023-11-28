-- POOL

CREATE OR REPLACE FUNCTION grestv0.pool_blocks(_pool_bech32 text, _epoch_no word31type DEFAULT NULL)
RETURNS TABLE (
  epoch_no word31type,
  epoch_slot word31type,
  abs_slot word63type,
  block_height word31type,
  block_hash text,
  block_time integer
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN query
  SELECT * FROM grest.pool_blocks(_pool_bech32, _epoch_no);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_delegators(_pool_bech32 text)
RETURNS TABLE (
  stake_address character varying,
  amount text,
  active_epoch_no bigint,
  latest_delegation_tx_hash text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY 
    SELECT * FROM grest.pool_delegators(_pool_bech32);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_delegators_history(_pool_bech32 text, _epoch_no word31type DEFAULT NULL)
RETURNS TABLE (
  stake_address character varying,
  amount text,
  epoch_no word31type
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY 
    SELECT * FROM grest.pool_delegators_history(_pool_bech32, _epoch_no);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_history(_pool_bech32 text, _epoch_no word31type DEFAULT NULL)
RETURNS TABLE (
  epoch_no bigint,
  active_stake text,
  active_stake_pct numeric,
  saturation_pct numeric,
  block_cnt bigint,
  delegator_cnt bigint,
  margin double precision,
  fixed_cost text,
  pool_fees text,
  deleg_rewards text,
  member_rewards text,
  epoch_ros numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY 
    SELECT * FROM grest.pool_history(_pool_bech32, _epoch_no);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_info(_pool_bech32_ids text [])
RETURNS TABLE (
  pool_id_bech32 character varying,
  pool_id_hex text,
  active_epoch_no bigint,
  vrf_key_hash text,
  margin double precision,
  fixed_cost text,
  pledge text,
  reward_addr character varying,
  owners character varying [],
  relays jsonb [],
  meta_url character varying,
  meta_hash text,
  meta_json jsonb,
  pool_status text,
  retiring_epoch word31type,
  op_cert text,
  op_cert_counter word63type,
  active_stake text,
  sigma numeric,
  block_count numeric,
  live_pledge text,
  live_stake text,
  live_delegators bigint,
  live_saturation numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY 
    SELECT * FROM grest.pool_info(_pool_bech32_ids);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_list()
RETURNS TABLE (
  pool_id_bech32 character varying,
  ticker character varying
)
LANGUAGE plpgsql
AS $$
# variable_conflict use_column
BEGIN
  RETURN QUERY (
    WITH
      -- Get last pool update for each pool
      _pool_list AS (
        SELECT
          DISTINCT ON (pic.pool_id_bech32) pool_id_bech32,
          pool_status
        FROM
          grest.pool_info_cache AS pic
        ORDER BY
          pic.pool_id_bech32,
          pic.tx_id DESC
      ),

      _pool_meta AS (
        SELECT
          DISTINCT ON (pic.pool_id_bech32) pool_id_bech32,
          pod.ticker_name
        FROM
          grest.pool_info_cache AS pic
          LEFT JOIN public.pool_offline_data AS pod ON pod.pmr_id = pic.meta_id
        WHERE pod.ticker_name IS NOT NULL
        ORDER BY
          pic.pool_id_bech32,
          pic.tx_id DESC
      )

    SELECT
      pl.pool_id_bech32,
      pm.ticker_name
    FROM
      _pool_list AS pl
      LEFT JOIN _pool_meta AS pm ON pl.pool_id_bech32 = pm.pool_id_bech32
    WHERE
      pool_status != 'retired'
  );
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_metadata(_pool_bech32_ids text [] DEFAULT NULL)
RETURNS TABLE (
  pool_id_bech32 character varying,
  meta_url character varying,
  meta_hash text,
  meta_json jsonb,
  pool_status text
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
    SELECT * FROM grest.pool_metadata(_pool_bech32_ids);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_relays()
RETURNS TABLE (
  pool_id_bech32 character varying,
  relays jsonb [],
  pool_status text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY 
    SELECT * FROM grest.pool_relays();
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_stake_snapshot(_pool_bech32 text)
RETURNS TABLE (
  snapshot text,
  epoch_no bigint,
  nonce text,
  pool_stake text,
  active_stake text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY 
    SELECT * FROM grest.pool_stake_snapshot(_pool_bech32);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.pool_updates(_pool_bech32 text DEFAULT NULL)
RETURNS TABLE (
  tx_hash text,
  block_time integer,
  pool_id_bech32 character varying,
  pool_id_hex text,
  active_epoch_no bigint,
  vrf_key_hash text,
  margin double precision,
  fixed_cost text,
  pledge text,
  reward_addr character varying,
  owners character varying [],
  relays jsonb [],
  meta_url character varying,
  meta_hash text,
  meta_json jsonb,
  pool_status text,
  retiring_epoch word31type
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  SELECT
    tx_hash,
    block_time::integer,
    pool_id_bech32,
    pool_id_hex,
    active_epoch_no,
    vrf_key_hash,
    margin,
    fixed_cost::text,
    pledge::text,
    reward_addr,
    owners,
    relays,
    meta_url,
    meta_hash,
    pod.json,
    pool_status,
    retiring_epoch
  FROM
    grest.pool_info_cache AS pic
    LEFT JOIN public.pool_offline_data AS pod ON pod.pmr_id = pic.meta_id
  WHERE
    _pool_bech32 IS NULL
    OR
    pool_id_bech32 = _pool_bech32
  ORDER BY
    tx_id DESC;
END;
$$;
