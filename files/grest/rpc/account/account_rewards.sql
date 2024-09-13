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
      SELECT ARRAY_AGG(DECODE(b32_decode(n), 'hex'))
      FROM UNNEST(_stake_addresses) AS n
    );

  IF _epoch_no IS NULL THEN
    RETURN QUERY
      SELECT
        grest.cip5_hex_to_stake_addr(sa.hash_raw),
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
          'earned_epoch', r.earned_epoch,
          'spendable_epoch', r.spendable_epoch,
          'amount', r.amount::text,
          'type', r.type,
          'pool_id', b32_encode('pool', DECODE(ph.hash_raw,'hex')::text)
          )
        ) AS rewards
      FROM
        reward AS r
        LEFT JOIN pool_hash AS ph ON r.pool_id = ph.id
        INNER JOIN stake_address AS sa ON sa.id = r.addr_id
      WHERE
        r.addr_id = ANY(sa_id_list)
      GROUP BY sa.hash_raw;
  ELSE
    RETURN QUERY
      SELECT
        grest.cip5_hex_to_stake_addr(sa.hash_raw),
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'earned_epoch', r.earned_epoch,
            'spendable_epoch', r.spendable_epoch,
            'amount', r.amount::text,
            'type', r.type,
            'pool_id', b32_encode('pool', DECODE(ph.hash_raw,'hex')::text)
          )
        ) AS rewards
      FROM
        reward AS r
        LEFT JOIN pool_hash AS ph ON r.pool_id = ph.id
        INNER JOIN stake_address AS sa ON sa.id = r.addr_id
      WHERE
        r.addr_id = ANY(sa_id_list)
        AND r.earned_epoch = _epoch_no
      GROUP BY sa.hash_raw;
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.account_rewards IS 'Get the full rewards history (including MIR) for given stake addresses, or certain epoch if specified'; -- noqa: LT01
