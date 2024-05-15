CREATE TABLE IF NOT EXISTS grest.stake_distribution_cache (
  stake_address varchar PRIMARY KEY,
  pool_id varchar,
  total_balance numeric,
  utxo numeric,
  rewards numeric,
  withdrawals numeric,
  rewards_available numeric
);

CREATE OR REPLACE PROCEDURE grest.update_stake_distribution_cache()
LANGUAGE plpgsql
AS $$
DECLARE -- Last block height to control future re-runs of the query
  _last_accounted_block_height bigint;
  _last_account_tx_id bigint;
  _active_stake_epoch bigint;
  _latest_epoch bigint;
  _row_count bigint;
BEGIN
  SELECT MAX(block_no) FROM public.block
    WHERE block_no IS NOT NULL INTO _last_accounted_block_height;
  SELECT (last_value::integer - 2)::integer INTO _active_stake_epoch FROM grest.control_table
    WHERE key = 'last_active_stake_validated_epoch';
  SELECT (eic.i_last_tx_id) INTO _last_account_tx_id
    FROM grest.epoch_info_cache AS eic
    WHERE eic.epoch_no = _active_stake_epoch;
  SELECT MAX(no) INTO _latest_epoch FROM public.epoch WHERE no IS NOT NULL;

  WITH
    accounts_with_delegated_pools AS (
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
          -- skip delegations that were followed by at least one stake pool retirement
          AND NOT grest.is_dangling_delegation(delegation.id)
          -- Account must be present in epoch_stake table for the last validated epoch
          AND EXISTS (
            SELECT TRUE
            FROM epoch_stake
            WHERE epoch_stake.epoch_no = (
                SELECT last_value::integer
                FROM grest.control_table
                WHERE key = 'last_active_stake_validated_epoch'
              )
              AND epoch_stake.addr_id = stake_address.id
          )
    ),

    pool_ids AS (
      SELECT
        awdp.stake_address_id,
        pool_hash.view AS pool_id
      FROM pool_hash
        INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.pool_hash_id = pool_hash.id
    ),

    account_active_stake AS (
      SELECT
        awdp.stake_address_id,
        es.amount
      FROM epoch_stake AS es
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address_id = es.addr_id
      WHERE epoch_no = (_active_stake_epoch + 2)
    ),

    account_delta_tx_ins AS (
      SELECT
        awdp.stake_address_id,
        tx_out.id AS txoid
      FROM tx_out
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address_id = tx_out.stake_address_id
      WHERE tx_out.consumed_by_tx_id > _last_account_tx_id
    ),

    account_delta_input AS (
      SELECT
        tx_out.stake_address_id,
        COALESCE(SUM(tx_out.value), 0) AS amount
      FROM account_delta_tx_ins
      LEFT JOIN tx_out ON account_delta_tx_ins.txoid=tx_out.id
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address_id = tx_out.stake_address_id
      GROUP BY tx_out.stake_address_id
    ),

    account_delta_output AS (
      SELECT
        awdp.stake_address_id,
        COALESCE(SUM(tx_out.value), 0) AS amount
      FROM tx_out
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address_id = tx_out.stake_address_id
      WHERE tx_out.tx_id > _last_account_tx_id
      GROUP BY awdp.stake_address_id
    ),

    account_delta_rewards AS (
      SELECT
        awdp.stake_address_id,
        COALESCE(SUM(reward.amount), 0) AS rewards
      FROM reward
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address_id = reward.addr_id
      WHERE (reward.spendable_epoch >= (_active_stake_epoch + 2) AND reward.spendable_epoch <= _latest_epoch )
        OR (reward.TYPE = 'refund' AND reward.spendable_epoch >= (_active_stake_epoch + 1) AND reward.spendable_epoch <= _latest_epoch )
      GROUP BY awdp.stake_address_id
    ),

    account_delta_instant_rewards AS (
      SELECT
        awdp.stake_address_id,
        COALESCE(SUM(ir.amount), 0) AS amount
      FROM instant_reward AS ir
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address_id = ir.addr_id
      WHERE ir.spendable_epoch >= (_active_stake_epoch + 2)
        AND ir.spendable_epoch <= _latest_epoch
      GROUP BY awdp.stake_address_id
    ),

    account_delta_withdrawals AS (
      SELECT
        accounts_with_delegated_pools.stake_address_id,
        COALESCE(SUM(withdrawal.amount), 0) AS withdrawals
      FROM withdrawal
      INNER JOIN accounts_with_delegated_pools ON accounts_with_delegated_pools.stake_address_id = withdrawal.addr_id
      WHERE withdrawal.tx_id > _last_account_tx_id
      GROUP BY accounts_with_delegated_pools.stake_address_id
    ),

    account_total_rewards AS (
      SELECT
        accounts_with_delegated_pools.stake_address_id,
        COALESCE(SUM(reward.amount), 0) AS rewards
      FROM reward
      INNER JOIN accounts_with_delegated_pools ON accounts_with_delegated_pools.stake_address_id = reward.addr_id
      WHERE reward.spendable_epoch <= _latest_epoch
      GROUP BY accounts_with_delegated_pools.stake_address_id
    ),

    account_total_withdrawals AS (
      SELECT
        accounts_with_delegated_pools.stake_address_id,
        COALESCE(SUM(withdrawal.amount), 0) AS withdrawals
      FROM withdrawal
      INNER JOIN accounts_with_delegated_pools ON accounts_with_delegated_pools.stake_address_id = withdrawal.addr_id
      GROUP BY accounts_with_delegated_pools.stake_address_id
    ),

    account_total_instant_rewards AS (
      SELECT
        awdp.stake_address_id,
        COALESCE(SUM(ir.amount), 0) AS amount
      FROM instant_reward AS ir
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address_id = ir.addr_id
      WHERE ir.spendable_epoch <= _latest_epoch
      GROUP BY awdp.stake_address_id
    )

  -- INSERT QUERY START
  INSERT INTO grest.stake_distribution_cache
    SELECT
      awdp.stake_address,
      pi.pool_id,
      COALESCE(aas.amount, 0) + COALESCE(ado.amount, 0) - COALESCE(adi.amount, 0) + COALESCE(adr.rewards, 0) + COALESCE(adir.amount, 0) - COALESCE(adw.withdrawals, 0) AS total_balance,
      COALESCE(aas.amount, 0) + COALESCE(ado.amount, 0) - COALESCE(adi.amount, 0) + COALESCE(adr.rewards, 0) + COALESCE(adir.amount, 0) - COALESCE(adw.withdrawals, 0) - COALESCE(atrew.rewards, 0) - COALESCE(atir.amount, 0) + COALESCE(atw.withdrawals, 0) AS utxo,
      COALESCE(atrew.rewards, 0) AS rewards,
      COALESCE(atw.withdrawals, 0) AS withdrawals,
      COALESCE(atrew.rewards, 0) + COALESCE(atir.amount, 0) - COALESCE(atw.withdrawals, 0) AS rewards_available
    FROM accounts_with_delegated_pools AS awdp
    INNER JOIN pool_ids AS pi ON pi.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_active_stake AS aas ON aas.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_total_rewards AS atrew ON atrew.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_total_withdrawals AS atw ON atw.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_total_instant_rewards AS atir ON atir.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_delta_input AS adi ON adi.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_delta_output AS ado ON ado.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_delta_rewards AS adr ON adr.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_delta_instant_rewards AS adir ON adir.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_delta_withdrawals AS adw ON adw.stake_address_id = awdp.stake_address_id
    ON CONFLICT (stake_address) DO
      UPDATE
        SET pool_id = excluded.pool_id,
          total_balance = excluded.total_balance,
          utxo = excluded.utxo,
          rewards = excluded.rewards,
          withdrawals = excluded.withdrawals,
          rewards_available = excluded.rewards_available
  ;

  INSERT INTO grest.control_table (key, last_value)
    VALUES (
        'stake_distribution_lbh',
        _last_accounted_block_height
      ) ON CONFLICT (key) DO
    UPDATE
    SET last_value = _last_accounted_block_height;

  
  -- Clean up de-registered accounts
  DELETE FROM grest.stake_distribution_cache
  WHERE stake_address IN (
    SELECT DISTINCT ON (sa.id)
      sa.view
    FROM stake_address AS sa
    INNER JOIN stake_deregistration AS sd ON sa.id = sd.addr_id
      WHERE NOT EXISTS (
        SELECT TRUE
        FROM stake_registration AS sr
        WHERE sr.addr_id = sd.addr_id
          AND sr.tx_id >= sd.tx_id
      )
  );

  -- Clean up accounts registered to retired-at-least-once-since pools
  RAISE NOTICE 'DANGLING delegation cleanup from SDC commencing';
  DELETE FROM grest.stake_distribution_cache
    WHERE stake_address in (
     SELECT z.stake_address
     FROM (
      SELECT 
        (
          SELECT max(d.id)
          FROM delegation d
            INNER JOIN stake_address sd ON sd.view = sdc.stake_address AND sd.id = d.addr_id) AS last_deleg, 
        sdc.stake_address
      FROM grest.stake_distribution_cache AS sdc
    ) AS z
    WHERE grest.is_dangling_delegation(z.last_deleg)
  );

  GET DIAGNOSTICS _row_count = ROW_COUNT;
  RAISE NOTICE 'DANGLING delegations - deleted % rows', _row_count;

END;
$$;

-- HELPER FUNCTION: grest.stake_distribution_cache_update_check
-- Determines whether or not the stake distribution cache should be updated
-- based ON the time rule (max once in 60 mins), and ensures previous run completed.

CREATE OR REPLACE FUNCTION grest.stake_distribution_cache_update_check()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  _last_update_block_height bigint DEFAULT NULL;
  _current_block_height bigint DEFAULT NULL;
  _last_update_block_diff bigint DEFAULT NULL;
BEGIN
  IF (
    -- If checking query with the same name there will be 2 results
    SELECT COUNT(pid) > 1
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query ILIKE '%grest.stake_distribution_cache_update_check(%'
      AND datname = (
        SELECT current_database()
      )
    ) THEN
      RAISE EXCEPTION 'Previous query still running but should have completed! Exiting...';
  ELSIF (
    -- If checking query with a different name there will be 1 result
    SELECT COUNT(pid) > 0
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query ILIKE '%grest.update_newly_registered_accounts_stake_distribution_cache(%'
      AND datname = (
        SELECT current_database()
      )
  ) THEN
    RAISE EXCEPTION 'New accounts query running! Exiting...';
  ELSIF (
    SELECT count(last_value) = 0
    FROM grest.control_table
    WHERE key = 'last_active_stake_validated_epoch'
    ) OR (
      SELECT ((SELECT MAX(no) FROM epoch) - COALESCE((last_value::integer - 2)::integer, 0 ))  > 2
      FROM grest.control_table
      WHERE key = 'last_active_stake_validated_epoch'
    ) THEN
    RAISE EXCEPTION 'Active Stake cache too far, skipping...';
  ELSIF (
    SELECT
      ((SELECT MAX(no) FROM epoch) - (SELECT MAX(epoch_no)::integer FROM grest.epoch_info_cache))::integer > 0
    ) THEN
    RAISE EXCEPTION 'Epoch Info cache wasnt run yet, skipping...';
  END IF;

  -- QUERY START --
  SELECT COALESCE(
      (
        SELECT last_value::bigint
        FROM grest.control_table
        WHERE key = 'stake_distribution_lbh'
      ), 0
    ) INTO _last_update_block_height;

  SELECT MAX(block_no)
    FROM public.block
    WHERE BLOCK_NO IS NOT NULL INTO _current_block_height;

  SELECT (_current_block_height - _last_update_block_height) INTO _last_update_block_diff;
  -- Do nothing until there is a 180 blocks difference in height - 60 minutes theoretical time
  -- 185 in check because last block height considered is 5 blocks behind tip

  Raise NOTICE 'Last stake distribution update was % blocks ago...',
    _last_update_block_diff;
    IF (_last_update_block_diff >= 180
        OR _last_update_block_diff < 0 -- Special case for db-sync restart rollback to epoch start
      ) THEN
      RAISE NOTICE 'Re-running...';
      CALL grest.update_stake_distribution_cache ();
    ELSE
      RAISE NOTICE 'Minimum block height difference(180) for update not reached, skipping...';
    END IF;

    RETURN;
  END;
$$;

DROP INDEX IF EXISTS grest.idx_pool_id;
CREATE INDEX idx_pool_id ON grest.stake_distribution_cache (pool_id);
-- Populated by first crontab execution
