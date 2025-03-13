CREATE OR REPLACE FUNCTION grest.account_stake_history(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  pool_id_bech32 varchar,
  epoch_no bigint,
  active_stake text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    sa.view::varchar AS stake_address,
    ph.view AS pool_id_bech32,
    es.epoch_no::bigint,
    es.amount::text AS active_stake
  FROM epoch_stake AS es
    INNER JOIN stake_address AS sa ON sa.id = es.addr_id
    INNER JOIN pool_hash AS ph ON ph.id = es.pool_id
  WHERE sa.view = ANY(
      SELECT UNNEST(_stake_addresses) AS p
    )
  ORDER by sa.id, es.epoch_no DESC;
$$;

COMMENT ON FUNCTION grest.account_stake_history IS 'Stake Accounts epoch-wise history of active stake'; -- noqa: LT01
