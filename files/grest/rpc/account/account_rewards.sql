CREATE OR REPLACE FUNCTION grest.account_rewards(_stake_addresses text [], _epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  stake_address varchar,
  rewards jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  sa_id_list integer[];
BEGIN
  SELECT INTO sa_id_list
    ARRAY_AGG(stake_address.id)
  FROM
    stake_address
  WHERE
    stake_address.hash_raw = ANY(
      SELECT cardano.bech32_decode_data(n)
      FROM UNNEST(_stake_addresses) AS n
    );

  RETURN QUERY
    SELECT
      grest.cip5_hex_to_stake_addr(all_rewards.stake_address_raw)::varchar,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'earned_epoch', all_rewards.earned_epoch,
          'spendable_epoch', all_rewards.spendable_epoch,
          'amount', all_rewards.amount::text,
          'type', all_rewards.type,
          'pool_id', CASE WHEN all_rewards.pool_id_raw IS NULL THEN NULL ELSE cardano.bech32_encode('pool', all_rewards.pool_id_raw) END
        )
      ) AS rewards
    FROM (
      SELECT
        sa.hash_raw as stake_address_raw,
        r.type,
        r.amount,
        r.earned_epoch,
        r.spendable_epoch,
        ph.hash_raw as pool_id_raw
      FROM reward AS r
        INNER JOIN pool_hash AS ph ON r.pool_id = ph.id
        INNER JOIN stake_address AS sa ON sa.id = r.addr_id
      WHERE r.addr_id = ANY(sa_id_list)
        AND CASE WHEN _epoch_no IS NULL THEN TRUE ELSE r.earned_epoch = _epoch_no END
      --
      UNION ALL
      --
      SELECT
        sa.hash_raw as stake_address_raw,
        rr.type,
        rr.amount,
        rr.earned_epoch,
        rr.spendable_epoch,
        null as pool_id_raw
      FROM reward_rest AS rr
        INNER JOIN stake_address AS sa ON sa.id = rr.addr_id
      WHERE rr.addr_id = ANY(sa_id_list)
        AND CASE WHEN _epoch_no IS NULL THEN TRUE ELSE rr.earned_epoch = _epoch_no END
    ) as all_rewards
    GROUP BY all_rewards.stake_address_raw;
END;
$$;

COMMENT ON FUNCTION grest.account_rewards IS 'DEPRECATED: Get the full rewards history (including MIR) for given stake addresses, or certain epoch if specified'; -- noqa: LT01
