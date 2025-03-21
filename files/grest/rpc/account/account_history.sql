CREATE OR REPLACE FUNCTION grest.account_history(_stake_addresses text [], _epoch_no integer DEFAULT NULL)
RETURNS TABLE (
  stake_address varchar,
  history jsonb
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

  IF _epoch_no IS NOT NULL THEN
    RETURN QUERY
      SELECT
        grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar AS stake_address,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'pool_id', cardano.bech32_encode('pool', ph.hash_raw),
            'epoch_no', es.epoch_no::bigint,
            'active_stake', es.amount::text
          )
        )
      FROM
        epoch_stake AS es
        LEFT JOIN stake_address AS sa ON sa.id = es.addr_id
        LEFT JOIN pool_hash AS ph ON ph.id = es.pool_id
      WHERE
        es.epoch_no = _epoch_no
        AND
        sa.id = ANY(sa_id_list)
      GROUP BY
        sa.hash_raw;
  ELSE
    RETURN QUERY
      SELECT
        grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar AS stake_address,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'pool_id', cardano.bech32_encode('pool', ph.hash_raw),
            'epoch_no', es.epoch_no::bigint,
            'active_stake', es.amount::text
          )
        )
      FROM
        epoch_stake AS es
        LEFT JOIN stake_address AS sa ON sa.id = es.addr_id
        LEFT JOIN pool_hash AS ph ON ph.id = es.pool_id
      WHERE
        sa.id = ANY(sa_id_list)
      GROUP BY
        sa.hash_raw;
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.account_history IS 'DEPRECATED: Get the active stake history of given accounts'; -- noqa: LT01
