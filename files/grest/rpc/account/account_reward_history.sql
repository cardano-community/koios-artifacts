CREATE OR REPLACE FUNCTION grest.account_reward_history(_stake_addresses text [], _epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  stake_address varchar,
  earned_epoch bigint,
  spendable_epoch bigint,
  amount text,
  type text,
  pool_id_bech32 varchar
)
LANGUAGE sql STABLE
AS $$
  WITH sa_id_list AS (
    SELECT
      id,
      grest.cip5_hex_to_stake_addr(hash_raw)::varchar AS stake_address
    FROM public.stake_address AS sa1
    WHERE sa1.hash_raw = ANY(
        SELECT cardano.bech32_decode_data(n)
        FROM UNNEST(_stake_addresses) AS n
      )
  )
  SELECT
    all_rewards.stake_address,
    all_rewards.earned_epoch,
    all_rewards.spendable_epoch,
    all_rewards.amount::text,
    all_rewards.type::text,
    all_rewards.pool_id_bech32::varchar
  FROM (
    SELECT
      sa.stake_address,
      r.type,
      r.amount,
      r.earned_epoch,
      r.spendable_epoch,
      cardano.bech32_encode('pool', ph.hash_raw) AS pool_id_bech32
    FROM sa_id_list AS sa
      INNER JOIN reward AS r ON r.addr_id = sa.id
      INNER JOIN pool_hash AS ph ON r.pool_id = ph.id
    WHERE CASE WHEN _epoch_no IS NULL THEN TRUE ELSE r.earned_epoch = _epoch_no END
    --
    UNION ALL
    --
    SELECT
      sa.stake_address,
      rr.type,
      rr.amount,
      rr.earned_epoch,
      rr.spendable_epoch,
      null AS pool_id_bech32
    FROM sa_id_list AS sa
      INNER JOIN reward_rest AS rr ON rr.addr_id = sa.id
    WHERE CASE WHEN _epoch_no IS NULL THEN TRUE ELSE rr.earned_epoch = _epoch_no END
  ) as all_rewards
  ORDER BY all_rewards.stake_address;
$$;

COMMENT ON FUNCTION grest.account_reward_history IS 'Get the full rewards history (including MIR) for given stake addresses, or certain epoch if specified'; -- noqa: LT01
