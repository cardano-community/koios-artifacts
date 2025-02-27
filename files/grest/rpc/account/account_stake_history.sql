CREATE OR REPLACE FUNCTION grest.account_stake_history(_stake_addresses text [], _epoch_no integer DEFAULT NULL)
RETURNS TABLE (
  stake_address varchar,
  pool_id_bech32 varchar,
  epoch_no bigint,
  active_stake text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar AS stake_address,
    ph.view AS pool_id_bech32,
    es.epoch_no::bigint,
    es.amount::text AS active_stake
  FROM epoch_stake AS es
    INNER JOIN stake_address AS sa ON sa.id = es.addr_id
    INNER JOIN pool_hash AS ph ON ph.id = es.pool_id
  WHERE es.addr_id = ANY(
      SELECT id FROM stake_address AS sa1 WHERE sa1.hash_raw = ANY(
        SELECT cardano.bech32_decode_data(p)
        FROM UNNEST(_stake_addresses) AS p)
    )
    AND (_epoch_no IS NOT NULL AND es.epoch_no = _epoch_no)
    OR (_epoch_no IS NULL)
  ORDER by sa.id, es.epoch_no DESC;
$$;

COMMENT ON FUNCTION grest.account_stake_history IS 'Stake Accounts epoch-wise history of active stake'; -- noqa: LT01
