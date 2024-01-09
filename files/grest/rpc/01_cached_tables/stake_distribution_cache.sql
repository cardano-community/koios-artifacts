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
  _last_active_stake_blockid bigint;
  _latest_epoch bigint;
BEGIN
  SELECT MAX(block_no) FROM public.block
    WHERE block_no IS NOT NULL INTO _last_accounted_block_height;
  SELECT (last_value::integer - 2)::integer INTO _active_stake_epoch FROM grest.control_table
    WHERE key = 'last_active_stake_validated_epoch';
  SELECT MAX(tx.id) INTO _last_account_tx_id
  FROM public.tx
  INNER JOIN block AS b ON b.id = tx.block_id
  WHERE b.epoch_no <= _active_stake_epoch
    AND b.block_no IS NOT NULL
    AND b.tx_count != 0;
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
        acsc.amount
      FROM grest.account_active_stake_cache AS acsc
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address = acsc.stake_address
      WHERE epoch_no = (_active_stake_epoch + 2)
    ),

    account_delta_tx_ins AS (
      SELECT
        awdp.stake_address_id,
        tx_in.tx_out_id AS txoid,
        tx_in.tx_out_index AS txoidx
      FROM tx_in
      LEFT JOIN tx_out ON tx_in.tx_out_id = tx_out.tx_id
        AND tx_in.tx_out_index::smallint = tx_out.index::smallint
      INNER JOIN accounts_with_delegated_pools AS awdp ON awdp.stake_address_id = tx_out.stake_address_id
      WHERE tx_in.tx_in_id > _last_account_tx_id
    ),

    account_delta_input AS (
      SELECT
        tx_out.stake_address_id,
        COALESCE(SUM(tx_out.value), 0) AS amount
      FROM account_delta_tx_ins
      LEFT JOIN tx_out
        ON account_delta_tx_ins.txoid=tx_out.tx_id
          AND account_delta_tx_ins.txoidx = tx_out.index
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
      WHERE
        (reward.spendable_epoch >= (_active_stake_epoch + 2) AND reward.spendable_epoch <= _latest_epoch )
        OR (reward.TYPE = 'refund' AND reward.spendable_epoch >= (_active_stake_epoch + 1) AND reward.spendable_epoch <= _latest_epoch )
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
    )

  -- INSERT QUERY START
  INSERT INTO grest.stake_distribution_cache
    SELECT
      awdp.stake_address,
      pi.pool_id,
      COALESCE(aas.amount, 0) + COALESCE(ado.amount, 0) - COALESCE(adi.amount, 0) + COALESCE(adr.rewards, 0) - COALESCE(adw.withdrawals, 0) AS total_balance,
      CASE
        WHEN ( COALESCE(atrew.rewards, 0) - COALESCE(atw.withdrawals, 0) ) <= 0 THEN
          COALESCE(aas.amount, 0) + COALESCE(ado.amount, 0) - COALESCE(adi.amount, 0) + COALESCE(adr.rewards, 0) - COALESCE(adw.withdrawals, 0)
        ELSE
          COALESCE(aas.amount, 0) + COALESCE(ado.amount, 0) - COALESCE(adi.amount, 0) + COALESCE(adr.rewards, 0) - COALESCE(adw.withdrawals, 0) - (COALESCE(atrew.rewards, 0) - COALESCE(atw.withdrawals, 0))
      END AS utxo,
      COALESCE(atrew.rewards, 0) AS rewards,
      COALESCE(atw.withdrawals, 0) AS withdrawals,
      CASE
        WHEN ( COALESCE(atrew.rewards, 0) - COALESCE(atw.withdrawals, 0) ) <= 0 THEN 0
        ELSE COALESCE(atrew.rewards, 0) - COALESCE(atw.withdrawals, 0)
      END AS rewards_available
    FROM accounts_with_delegated_pools AS awdp
    INNER JOIN pool_ids AS pi ON pi.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_active_stake AS aas ON aas.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_total_rewards AS atrew ON atrew.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_total_withdrawals AS atw ON atw.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_delta_input AS adi ON adi.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_delta_output AS ado ON ado.stake_address_id = awdp.stake_address_id
    LEFT JOIN account_delta_rewards AS adr ON adr.stake_address_id = awdp.stake_address_id
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
      SELECT ((SELECT MAX(no) FROM epoch) - COALESCE((last_value::integer - 2)::integer, 0 ))  > 3
      FROM grest.control_table
      WHERE key = 'last_active_stake_validated_epoch'
    ) THEN
    RAISE EXCEPTION 'Active Stake cache too far, skipping...';
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
