CREATE OR REPLACE FUNCTION grest.account_info(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  status text,
  delegated_pool varchar,
  total_balance text,
  utxo text,
  rewards text,
  withdrawals text,
  rewards_available text,
  reserves text,
  treasury text
)
LANGUAGE plpgsql
AS $$
DECLARE
  sa_id_list integer[] DEFAULT NULL;
BEGIN
  SELECT INTO sa_id_list
    array_agg(id)
  FROM stake_address
  WHERE stake_address.view = ANY(_stake_addresses);

  RETURN QUERY
    WITH latest_withdrawal_txs AS (
      SELECT DISTINCT ON (addr_id)
        addr_id,
        tx_id
      FROM withdrawal
      WHERE addr_id = ANY(sa_id_list)
      ORDER BY addr_id, tx_id DESC
    ),

    latest_withdrawal_epochs AS (
      SELECT
        lwt.addr_id,
        b.epoch_no
      FROM block b
        INNER JOIN tx ON tx.block_id = b.id
        INNER JOIN latest_withdrawal_txs AS lwt ON tx.id = lwt.tx_id
    )

    SELECT
      status_t.view AS stake_address,
      CASE WHEN status_t.registered = TRUE THEN
        'registered'
      ELSE
        'not registered'
      END AS status,
      pool_t.delegated_pool,
      CASE WHEN (COALESCE(rewards_t.rewards, 0) - COALESCE(withdrawals_t.withdrawals, 0)) < 0 THEN
        (COALESCE(utxo_t.utxo, 0) + COALESCE(rewards_t.rewards, 0) - COALESCE(withdrawals_t.withdrawals, 0) + COALESCE(reserves_t.reserves, 0) + COALESCE(treasury_t.treasury, 0) - (COALESCE(rewards_t.rewards, 0) - COALESCE(withdrawals_t.withdrawals, 0)))::text
      ELSE
        (COALESCE(utxo_t.utxo, 0) + COALESCE(rewards_t.rewards, 0) - COALESCE(withdrawals_t.withdrawals, 0) + COALESCE(reserves_t.reserves, 0) + COALESCE(treasury_t.treasury, 0))::text
      END AS total_balance,
      COALESCE(utxo_t.utxo, 0)::text AS utxo,
      COALESCE(rewards_t.rewards, 0)::text AS rewards,
      COALESCE(withdrawals_t.withdrawals, 0)::text AS withdrawals,
      CASE WHEN (COALESCE(rewards_t.rewards, 0) - COALESCE(withdrawals_t.withdrawals, 0)) <= 0 THEN
        '0'
      ELSE
        (COALESCE(rewards_t.rewards, 0) - COALESCE(withdrawals_t.withdrawals, 0))::text
      END AS rewards_available,
      COALESCE(reserves_t.reserves, 0)::text AS reserves,
      COALESCE(treasury_t.treasury, 0)::text AS treasury
    FROM
      (
        SELECT
          sa.id,
          sa.view,
          EXISTS (
            SELECT TRUE FROM stake_registration
            WHERE
              stake_registration.addr_id = sa.id
              AND NOT EXISTS (
                SELECT TRUE
                FROM stake_deregistration
                WHERE
                  stake_deregistration.addr_id = stake_registration.addr_id
                  AND stake_deregistration.tx_id > stake_registration.tx_id
              )
          ) AS registered
        FROM public.stake_address sa
        WHERE sa.id = ANY(sa_id_list)
      ) AS status_t
    LEFT JOIN (
        SELECT
          delegation.addr_id,
          pool_hash.view AS delegated_pool
        FROM delegation
          INNER JOIN pool_hash ON pool_hash.id = delegation.pool_hash_id
        WHERE
          delegation.addr_id = ANY(sa_id_list)
          AND NOT EXISTS (
            SELECT TRUE
            FROM delegation AS d
            WHERE d.addr_id = delegation.addr_id
              AND d.id > delegation.id)
            AND NOT EXISTS (
              SELECT TRUE
              FROM stake_deregistration
              WHERE stake_deregistration.addr_id = delegation.addr_id
                AND stake_deregistration.tx_id > delegation.tx_id)
            -- skip delegations that were followed by at least one pool retirement
            AND NOT grest.is_dangling_delegation(delegation.id)
      ) AS pool_t ON pool_t.addr_id = status_t.id
    LEFT JOIN (
        SELECT
          tx_out.stake_address_id,
          COALESCE(SUM(VALUE), 0) AS utxo
        FROM tx_out
        WHERE tx_out.stake_address_id = ANY(sa_id_list)
          AND tx_out.consumed_by_tx_id IS NULL
        GROUP BY tx_out.stake_address_id
      ) AS utxo_t ON utxo_t.stake_address_id = status_t.id
    LEFT JOIN (
        SELECT
          reward.addr_id,
          COALESCE(SUM(reward.amount), 0) AS rewards
        FROM reward
        WHERE reward.addr_id = ANY(sa_id_list)
          AND reward.spendable_epoch <= (
            SELECT MAX(no)
            FROM epoch
          )
        GROUP BY
          reward.addr_id
      ) AS rewards_t ON rewards_t.addr_id = status_t.id
    LEFT JOIN (
        SELECT
          withdrawal.addr_id,
          COALESCE(SUM(withdrawal.amount), 0) AS withdrawals
        FROM withdrawal
        WHERE withdrawal.addr_id = ANY(sa_id_list)
        GROUP BY
          withdrawal.addr_id
      ) AS withdrawals_t ON withdrawals_t.addr_id = status_t.id
    LEFT JOIN (
        SELECT
          reserve.addr_id,
          COALESCE(SUM(reserve.amount), 0) AS reserves
        FROM reserve
          INNER JOIN tx ON tx.id = reserve.tx_id
          INNER JOIN block ON block.id = tx.block_id
          INNER JOIN latest_withdrawal_epochs AS lwe ON lwe.addr_id = reserve.addr_id
        WHERE reserve.addr_id = ANY(sa_id_list)
          AND block.epoch_no >= lwe.epoch_no
        GROUP BY
          reserve.addr_id
      ) AS reserves_t ON reserves_t.addr_id = status_t.id
    LEFT JOIN (
        SELECT
          treasury.addr_id,
          COALESCE(SUM(treasury.amount), 0) AS treasury
        FROM treasury
          INNER JOIN tx ON tx.id = treasury.tx_id
          INNER JOIN block ON block.id = tx.block_id
          INNER JOIN latest_withdrawal_epochs AS lwe ON lwe.addr_id = treasury.addr_id
        WHERE treasury.addr_id = ANY(sa_id_list)
          AND block.epoch_no >= lwe.epoch_no
        GROUP BY
          treasury.addr_id
      ) AS treasury_t ON treasury_t.addr_id = status_t.id;
END;
$$;

COMMENT ON FUNCTION grest.account_info IS 'Get the account info for given stake addresses'; -- noqa: LT01
