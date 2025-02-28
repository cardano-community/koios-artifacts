CREATE OR REPLACE FUNCTION grest.account_info(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  status text,
  delegated_pool varchar,
  delegated_drep text,
  total_balance text,
  utxo text,
  rewards text,
  withdrawals text,
  rewards_available text,
  deposit text,
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
  WHERE stake_address.hash_raw = ANY(
    SELECT cardano.bech32_decode_data(n)
    FROM UNNEST(_stake_addresses) AS n
  );

  RETURN QUERY
  
    SELECT
      grest.cip5_hex_to_stake_addr(status_t.hash_raw)::varchar AS stake_address,
      CASE WHEN status_t.registered = TRUE THEN
        'registered'
      ELSE
        'not registered'
      END AS status,
      pool_t.delegated_pool,
      vote_t.delegated_drep,
      (COALESCE(utxo_t.utxo, 0) + COALESCE(rewards_t.rewards, 0) + COALESCE(reserves_t.reserves, 0) + COALESCE(treasury_t.treasury, 0) - COALESCE(withdrawals_t.withdrawals, 0))::text AS total_balance,
      COALESCE(utxo_t.utxo, 0)::text AS utxo,
      COALESCE(rewards_t.rewards, 0)::text AS rewards,
      COALESCE(withdrawals_t.withdrawals, 0)::text AS withdrawals,
      (COALESCE(rewards_t.rewards, 0) + COALESCE(reserves_t.reserves, 0) + COALESCE(treasury_t.treasury, 0) - COALESCE(withdrawals_t.withdrawals, 0))::text AS rewards_available,
      COALESCE(status_t.deposit,0)::text AS deposit,
      COALESCE(reserves_t.reserves, 0)::text AS reserves,
      COALESCE(treasury_t.treasury, 0)::text AS treasury
    FROM
      (
        SELECT
          sa.id,
          sa.hash_raw,
          EXISTS (
            SELECT TRUE FROM stake_registration AS sr
            WHERE sr.addr_id = sa.id
              AND NOT EXISTS (
                SELECT TRUE
                FROM stake_deregistration AS sd
                WHERE
                  sd.addr_id = sr.addr_id
                  AND sd.tx_id > sr.tx_id
                LIMIT 1
              )
          ) AS registered,
          (
            SELECT sr.deposit FROM stake_registration AS sr
            WHERE sr.addr_id = sa.id
              AND NOT EXISTS (
                SELECT TRUE
                FROM stake_deregistration AS sd
                WHERE
                  sd.addr_id = sr.addr_id
                  AND sd.tx_id > sr.tx_id
                LIMIT 1
              )
          ) AS deposit
        FROM public.stake_address sa
        WHERE sa.id = ANY(sa_id_list)
      ) AS status_t
    LEFT JOIN (
        SELECT
          dv.addr_id,
          COALESCE(grest.cip129_hex_to_drep_id(dh.raw, dh.has_script), dh.view::text) AS delegated_drep
        FROM delegation_vote AS dv
          INNER JOIN drep_hash AS dh ON dh.id = dv.drep_hash_id
        WHERE dv.addr_id = ANY(sa_id_list)
          AND NOT EXISTS (
            SELECT TRUE
            FROM delegation_vote AS dv1
            WHERE dv1.addr_id = dv.addr_id
              AND dv1.id > dv.id
            LIMIT 1)
          AND NOT EXISTS (
            SELECT TRUE
            FROM stake_deregistration
            WHERE stake_deregistration.addr_id = dv.addr_id
              AND stake_deregistration.tx_id > dv.tx_id
            LIMIT 1)
          AND NOT EXISTS (
            SELECT TRUE
            FROM drep_registration
            WHERE drep_registration.drep_hash_id = dv.drep_hash_id
              AND drep_registration.tx_id > dv.tx_id
              AND drep_registration.deposit < 0
            LIMIT 1)
      ) AS vote_t ON vote_t.addr_id = status_t.id
    LEFT JOIN (
        SELECT
          delegation.addr_id,
          cardano.bech32_encode('pool', ph.hash_raw)::varchar AS delegated_pool
        FROM delegation
          INNER JOIN pool_hash AS ph ON ph.id = delegation.pool_hash_id
        WHERE delegation.addr_id = ANY(sa_id_list)
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
          r.addr_id,
          COALESCE(SUM(r.amount), 0) AS reserves
        FROM reward_rest AS r
        WHERE r.addr_id = ANY(sa_id_list)
          AND r.type = 'reserves'
          AND r.spendable_epoch <= (
            SELECT MAX(no)
            FROM epoch
          )
        GROUP BY
          r.addr_id
      ) AS reserves_t ON reserves_t.addr_id = status_t.id
    LEFT JOIN (
        SELECT
          t.addr_id,
          COALESCE(SUM(t.amount), 0) AS treasury
        FROM reward_rest AS t
        WHERE t.addr_id = ANY(sa_id_list)
          AND t.type = 'treasury'
          AND t.spendable_epoch <= (
            SELECT MAX(no)
            FROM epoch
          )
        GROUP BY
          t.addr_id
      ) AS treasury_t ON treasury_t.addr_id = status_t.id
    ;
END;
$$;

COMMENT ON FUNCTION grest.account_info IS 'Get the account info for given stake addresses'; -- noqa: LT01
