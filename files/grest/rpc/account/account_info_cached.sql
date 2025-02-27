CREATE OR REPLACE FUNCTION grest.account_info_cached(_stake_addresses text [])
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
  FROM
    stake_address
  WHERE
    stake_address.hash_raw = ANY(
      SELECT cardano.bech32_decode_data(n)
      FROM UNNEST(_stake_addresses) AS n
    );

  RETURN QUERY

    SELECT
      grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar,
      CASE  WHEN status_t.registered = TRUE THEN
        'registered'
      ELSE
        'not registered'
      END AS status,
      cardano.bech32_encode('pool', ph.hash_raw)::varchar AS delegated_pool,
      vote_t.delegated_drep,
      sdc.total_balance::text,
      sdc.utxo::text,
      sdc.rewards::text,
      sdc.withdrawals::text,
      sdc.rewards_available::text,
      COALESCE(status_t.deposit,0)::text AS deposit,
      COALESCE(reserves_t.reserves, 0)::text AS reserves,
      COALESCE(treasury_t.treasury, 0)::text AS treasury
    FROM grest.stake_distribution_cache AS sdc
      INNER JOIN stake_address AS sa ON sa.id = sdc.stake_address_id
      INNER JOIN pool_hash AS ph ON sdc.pool_id = ph.id
      LEFT JOIN (
        SELECT
          sas.id,
          EXISTS (
            SELECT TRUE FROM stake_registration
            WHERE
              stake_registration.addr_id = sas.id
              AND NOT EXISTS (
                SELECT TRUE
                FROM stake_deregistration
                WHERE stake_deregistration.addr_id = stake_registration.addr_id
                  AND stake_deregistration.tx_id > stake_registration.tx_id
              )
          ) AS registered,
          (
            SELECT sr.deposit FROM stake_registration AS sr
            WHERE sr.addr_id = sas.id
              AND NOT EXISTS (
                SELECT TRUE
                FROM stake_deregistration AS sd
                WHERE
                  sd.addr_id = sr.addr_id
                  AND sd.tx_id > sr.tx_id
              )
          ) AS deposit
        FROM public.stake_address AS sas
        WHERE sas.id = ANY(sa_id_list)
        ) AS status_t ON sdc.stake_address_id = status_t.id
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
    WHERE sdc.stake_address_id = ANY(sa_id_list)

    UNION ALL

    SELECT 
      z.stake_address,
      ai.status,
      ai.delegated_pool AS pool_id,
      ai.delegated_drep,
      ai.total_balance::text,
      ai.utxo::text,
      ai.rewards::text,
      ai.withdrawals::text,
      ai.rewards_available::text,
      ai.deposit,
      ai.reserves,
      ai.treasury
      FROM
        (
          SELECT
            grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar AS stake_address,
            sa.id AS addr_id
          FROM stake_address AS sa 
          WHERE sa.id = ANY(sa_id_list)
           AND NOT EXISTS (SELECT null FROM grest.stake_distribution_cache AS sdc WHERE sdc.stake_address_id = sa.id)
        ) AS z
        , LATERAL grest.account_info(array[z.stake_address]) AS ai
    ;

END;
$$;

COMMENT ON FUNCTION grest.account_info_cached IS 'Get the cached account information for given stake addresses'; -- noqa: LT01
