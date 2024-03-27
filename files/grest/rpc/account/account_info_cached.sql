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
      FROM block AS b
        INNER JOIN tx ON tx.block_id = b.id
        INNER JOIN latest_withdrawal_txs AS lwt ON tx.id = lwt.tx_id
    )

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
          reserve.addr_id,
          COALESCE(SUM(reserve.amount), 0) AS reserves
        FROM reserve
          INNER JOIN tx ON tx.id = reserve.tx_id
          INNER JOIN block ON block.id = tx.block_id
          INNER JOIN latest_withdrawal_epochs AS lwe ON lwe.addr_id = reserve.addr_id
        WHERE reserve.addr_id = ANY(sa_id_list)
          AND block.epoch_no >= lwe.epoch_no
        GROUP BY reserve.addr_id
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
      COALESCE(reserves_t.reserves, 0)::text AS reserves,
      COALESCE(treasury_t.treasury, 0)::text AS treasury
      FROM
        (
          SELECT
            sa.view AS stake_address,
            sa.id AS addr_id
          FROM stake_address AS sa 
          WHERE view = ANY(_stake_addresses)
           AND NOT EXISTS (SELECT null FROM grest.stake_distribution_cache AS sdc WHERE sdc.stake_address = sa.view)
        ) AS z
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
          GROUP BY reserve.addr_id
          ) AS reserves_t ON reserves_t.addr_id = z.addr_id
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
          ) AS treasury_t ON treasury_t.addr_id = z.addr_id
        , LATERAL grest.account_info(array[z.stake_address]) AS ai
    ;

END;
$$;

COMMENT ON FUNCTION grest.account_info IS 'Get the cached account information for given stake addresses'; -- noqa: LT01
