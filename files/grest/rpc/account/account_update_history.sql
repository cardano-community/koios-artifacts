CREATE OR REPLACE FUNCTION grest.account_update_history(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  action_type text,
  tx_hash text,
  epoch_no bigint,
  epoch_slot bigint,
  absolute_slot bigint,
  block_time bigint
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
  ),
  actions AS (
    SELECT
      'registration' AS action_type,
      tx_id,
      addr_id
    FROM stake_registration
    WHERE addr_id IN (SELECT id FROM sa_id_list)
    UNION (
      SELECT
        'deregistration' AS action_type,
        tx_id,
        addr_id
      FROM stake_deregistration
      WHERE addr_id IN (SELECT id FROM sa_id_list)
    )
    UNION (
      SELECT
        'delegation_pool' AS action_type,
        tx_id,
        addr_id
      FROM delegation
      WHERE addr_id IN (SELECT id FROM sa_id_list)
    )
    UNION (
      SELECT
        'delegation_drep' AS action_type,
        tx_id,
        addr_id
      FROM delegation_vote
      WHERE addr_id IN (SELECT id FROM sa_id_list)
    )
    UNION (
      SELECT
        'withdrawal' AS action_type,
        tx_id,
        addr_id
      FROM withdrawal
      WHERE addr_id IN (SELECT id FROM sa_id_list)
    )
  )
  SELECT
    sa.stake_address,
    actions.action_type,
    ENCODE(tx.hash, 'hex'),
    b.epoch_no::bigint,
    b.epoch_slot_no::bigint,
    b.slot_no::bigint,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time
  FROM sa_id_list AS sa
    INNER JOIN actions ON sa.id = actions.addr_id
    INNER JOIN tx ON tx.id = actions.tx_id
    INNER JOIN block AS b ON b.id = tx.block_id
  ORDER BY sa.stake_address, b.slot_no DESC;
$$;

COMMENT ON FUNCTION grest.account_update_history IS 'Get historical updates (registration, deregistration, delegation and withdrawals) for given stake addresses'; -- noqa: LT01
