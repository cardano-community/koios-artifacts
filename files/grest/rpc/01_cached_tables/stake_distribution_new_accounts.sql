CREATE OR REPLACE PROCEDURE grest.update_newly_registered_accounts_stake_distribution_cache()
LANGUAGE plpgsql
AS $$
BEGIN
  IF (
      -- If checking query with a different name there will be 1 result
      SELECT COUNT(pid) > 1
      FROM pg_stat_activity
      WHERE state = 'active'
        AND query ILIKE '%grest.update_newly_registered_accounts_stake_distribution_cache(%'
        AND query NOT ILIKE '%pg_stat_activity%'
        AND datname = (
          SELECT current_database()
        )
    ) THEN
      RAISE EXCEPTION 'New accounts query already running! Exiting...';
  ELSIF (
    -- If checking query with a different name there will be 1 result
    SELECT COUNT(pid) > 0
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query ILIKE '%grest.stake_distribution_cache_update_check(%'
      AND datname = (
        SELECT current_database()
      )
  ) THEN
    RAISE NOTICE 'Stake distribution query running! Killing it and running new accounts update...';
    CALL grest.kill_queries_partial_match('grest.stake_distribution_cache_update_check(');
  END IF;

  WITH
    newly_registered_accounts AS (
      SELECT DISTINCT ON (stake_address.id)
        stake_address.id AS stake_address_id,
        stake_address.view AS stake_address,
        pool_hash_id
      FROM stake_address
      INNER JOIN delegation ON delegation.addr_id = stake_address.id
      WHERE
        NOT EXISTS (
          SELECT TRUE
          FROM delegation AS d
          WHERE d.addr_id = delegation.addr_id AND d.id > delegation.id
        )
        AND NOT EXISTS (
          SELECT TRUE
          FROM stake_deregistration
          WHERE stake_deregistration.addr_id = delegation.addr_id
            AND stake_deregistration.tx_id > delegation.tx_id
        )
        AND NOT EXISTS (
          SELECT TRUE
          FROM epoch_stake
          WHERE epoch_stake.epoch_no <= COALESCE(
            (
              SELECT last_value::integer
                FROM grest.control_table
                WHERE key = 'stake_distribution_new_epoch'
            ), (
              SELECT last_value::integer
                FROM grest.control_table
                WHERE key = 'last_active_stake_validated_epoch'
            ))
            AND epoch_stake.addr_id = stake_address.id
        )
    )
  -- INSERT QUERY START
  INSERT INTO grest.stake_distribution_cache
    SELECT
      nra.stake_address,
      ai.delegated_pool AS pool_id,
      ai.total_balance::lovelace,
      ai.utxo::lovelace,
      ai.rewards::lovelace,
      ai.withdrawals::lovelace,
      ai.rewards_available::lovelace
    FROM newly_registered_accounts AS nra,
      LATERAL grest.account_info(array[nra.stake_address]) AS ai
    ON CONFLICT (stake_address) DO
      UPDATE
        SET
          pool_id = EXCLUDED.pool_id,
          total_balance = EXCLUDED.total_balance,
          utxo = EXCLUDED.utxo,
          rewards = EXCLUDED.rewards,
          withdrawals = EXCLUDED.withdrawals,
          rewards_available = EXCLUDED.rewards_available;

  INSERT INTO grest.control_table (key, last_value)
    VALUES (
        'stake_distribution_new_epoch',
        (SELECT last_value::integer FROM grest.control_table
              WHERE key = 'last_active_stake_validated_epoch')
      ) ON CONFLICT (key) DO
    UPDATE
    SET last_value = (SELECT last_value::integer FROM grest.control_table
              WHERE key = 'last_active_stake_validated_epoch');

END;
$$;
