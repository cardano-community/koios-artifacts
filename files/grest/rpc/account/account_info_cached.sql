CREATE OR REPLACE FUNCTION grest.account_info_cached(_stake_addresses text [])
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
  FROM
    stake_address
  WHERE
    stake_address.view = ANY(_stake_addresses);

  RETURN QUERY

    SELECT
      sdc.stake_address,
      CASE  WHEN status_t.registered = TRUE THEN
        'registered'
      ELSE
        'not registered'
      END AS status,
      sdc.pool_id AS pool_id,
      sdc.total_balance::text,
      sdc.utxo::text,
      sdc.rewards::text,
      sdc.withdrawals::text,
      sdc.rewards_available::text,
      COALESCE(reserves_t.reserves, 0)::text AS reserves,
      COALESCE(treasury_t.treasury, 0)::text AS treasury
    FROM grest.stake_distribution_cache AS sdc
      LEFT JOIN (
        SELECT
          sas.id,
          sas.view,
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
          ) AS registered
        FROM public.stake_address AS sas
        WHERE sas.id = ANY(sa_id_list)
        ) AS status_t ON sdc.stake_address = status_t.view
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
    WHERE sdc.stake_address = ANY(_stake_addresses)

    UNION ALL

    SELECT 
      z.stake_address,
      ai.status,
      ai.delegated_pool AS pool_id,
      ai.total_balance::text,
      ai.utxo::text,
      ai.rewards::text,
      ai.withdrawals::text,
      ai.rewards_available::text,
      ai.reserves,
      ai.treasury
      FROM
        (
          SELECT
            sa.view AS stake_address,
            sa.id AS addr_id
          FROM stake_address AS sa 
          WHERE view = ANY(_stake_addresses)
           AND NOT EXISTS (SELECT null FROM grest.stake_distribution_cache AS sdc WHERE sdc.stake_address = sa.view)
        ) AS z
        , LATERAL grest.account_info(array[z.stake_address]) AS ai
    ;

END;
$$;

COMMENT ON FUNCTION grest.account_info IS 'Get the cached account information for given stake addresses'; -- noqa: LT01
