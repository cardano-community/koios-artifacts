CREATE TABLE IF NOT EXISTS grest.pool_info_cache (
  id serial PRIMARY KEY,
  tx_id bigint NOT NULL,
  update_id bigint NOT NULL,
  tx_hash text,
  block_time numeric,
  pool_hash_id bigint NOT NULL UNIQUE,
  active_epoch_no bigint NOT NULL,
  vrf_key_hash text NOT NULL,
  margin double precision NOT NULL,
  fixed_cost lovelace NOT NULL,
  pledge lovelace NOT NULL,
  deposit lovelace,
  reward_addr bigint,
  owners bigint [],
  relays jsonb [],
  meta_id bigint,
  meta_url varchar,
  meta_hash text,
  pool_status text,
  retiring_epoch word31type
);

COMMENT ON TABLE grest.pool_info_cache IS 'A summary of all pool parameters and updates';

CREATE OR REPLACE PROCEDURE grest.update_pool_info_cache()
LANGUAGE plpgsql
AS $$
DECLARE
  _current_block_height bigint;
  _current_epoch_no word31type;
  _pool_info_cache_last_block_height bigint;
  _pool_info_cache_last_tx_id bigint;
BEGIN
  SELECT MAX(block_no) FROM public.block
    WHERE block_no IS NOT NULL INTO _current_block_height;
  SELECT MAX(no) FROM public.epoch into _current_epoch_no;
  SELECT COALESCE(last_value::bigint, 0) INTO _pool_info_cache_last_block_height FROM grest.control_table
    WHERE key = 'pool_info_cache_last_block_height';
  SELECT COALESCE(MAX(tx.id), 0) INTO _pool_info_cache_last_tx_id
    FROM public.tx
    INNER JOIN public.block ON block.id = tx.block_id
    WHERE block.block_no <= _pool_info_cache_last_block_height;

  WITH
    latest_pool_updates AS (
      SELECT DISTINCT ON (pu.hash_id)
        pu.id,
        pu.hash_id,
        pu.cert_index,
        pu.vrf_key_hash,
        pu.pledge,
        pu.active_epoch_no,
        pu.meta_id,
        pu.margin,
        pu.fixed_cost,
        pu.registered_tx_id,
        pu.reward_addr_id,
        pu.deposit,
        pr.retiring_epoch
      FROM public.pool_update AS pu
      LEFT JOIN public.pool_retire AS pr ON
        pr.hash_id = pu.hash_id AND
        pr.announced_tx_id > pu.registered_tx_id
      WHERE pu.registered_tx_id > _pool_info_cache_last_tx_id
      ORDER BY hash_id, registered_tx_id, cert_index DESC
    )

    INSERT INTO grest.pool_info_cache (
      tx_id,
      update_id,
      tx_hash,
      block_time,
      pool_hash_id,
      active_epoch_no,
      vrf_key_hash,
      margin,
      fixed_cost,
      pledge,
      deposit,
      reward_addr,
      owners,
      relays,
      meta_id,
      meta_url,
      meta_hash,
      pool_status,
      retiring_epoch
    ) SELECT
        lpu.registered_tx_id,
        lpu.id,
        encode(tx.hash::bytea, 'hex'),
        EXTRACT(EPOCH FROM b.time),
        lpu.hash_id,
        lpu.active_epoch_no,
        ENCODE(lpu.vrf_key_hash::bytea, 'hex'),
        lpu.margin,
        lpu.fixed_cost,
        lpu.pledge,
        lpu.deposit,
        lpu.reward_addr_id,
        ARRAY(
          SELECT po.addr_id
          FROM public.pool_owner AS po
          WHERE po.pool_update_id = lpu.id
        ),
        ARRAY(
          SELECT JSONB_BUILD_OBJECT(
            'ipv4', pr.ipv4,
            'ipv6', pr.ipv6,
            'dns', pr.dns_name,
            'srv', pr.dns_srv_name,
            'port', pr.port
          )
          FROM public.pool_relay AS pr
          WHERE pr.update_id = lpu.id
        ),
        lpu.meta_id,
        pmr.url,
        encode(pmr.hash::bytea, 'hex'),
        CASE
          WHEN lpu.retiring_epoch IS NULL THEN 'registered'
          WHEN lpu.retiring_epoch > _current_epoch_no THEN 'retiring'
          ELSE 'retired'
        END,
        lpu.retiring_epoch
      FROM latest_pool_updates as lpu
      INNER JOIN public.pool_hash AS ph ON ph.id = lpu.hash_id
      INNER JOIN public.tx ON tx.id = lpu.registered_tx_id
      INNER JOIN public.block AS b ON b.id = tx.block_id
      LEFT JOIN public.pool_metadata_ref AS pmr ON pmr.id = lpu.meta_id
      ON CONFLICT (pool_hash_id) DO UPDATE
      SET
          tx_id = EXCLUDED.tx_id,
          update_id = EXCLUDED.update_id,
          tx_hash = EXCLUDED.tx_hash,
          block_time = EXCLUDED.block_time,
          active_epoch_no = EXCLUDED.active_epoch_no,
          vrf_key_hash = EXCLUDED.vrf_key_hash,
          margin = EXCLUDED.margin,
          fixed_cost = EXCLUDED.fixed_cost,
          pledge = EXCLUDED.pledge,
          deposit = EXCLUDED.deposit,
          reward_addr = EXCLUDED.reward_addr,
          owners = EXCLUDED.owners,
          relays = EXCLUDED.relays,
          meta_id = EXCLUDED.meta_id,
          meta_url = EXCLUDED.meta_url,
          meta_hash = EXCLUDED.meta_hash,
          pool_status = EXCLUDED.pool_status;

    INSERT INTO grest.control_table (key, last_value)
      VALUES (
          'pool_info_cache_last_block_height',
          _current_block_height
        ) ON CONFLICT (key) DO
      UPDATE
      SET last_value = _current_block_height;
END;
$$;


CREATE OR REPLACE FUNCTION grest.pool_info_cache_update_check()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  _last_update_block_height bigint DEFAULT NULL;
  _current_block_height bigint DEFAULT NULL;
  _last_update_block_diff bigint DEFAULT NULL;
BEGIN
  IF (
    SELECT COUNT(pid) > 1
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query ILIKE '%grest.pool_info_cache_update_check(%'
      AND datname = (
        SELECT current_database()
      )
    )
  THEN RAISE EXCEPTION 'Previous query still running but should have completed! Exiting...';
  END IF;
  
  SELECT COALESCE(
      (
        SELECT last_value::bigint
        FROM grest.control_table
        WHERE key = 'pool_info_cache_last_block_height'
      ),
      0
    ) INTO _last_update_block_height;

  SELECT MAX(block_no)
    FROM public.block
    WHERE BLOCK_NO IS NOT NULL INTO _current_block_height;

  SELECT (_current_block_height - _last_update_block_height) INTO _last_update_block_diff;

  Raise NOTICE 'Last pool info cache was % blocks ago...', _last_update_block_diff;
    IF (
      _last_update_block_diff >= 45 
      OR _last_update_block_diff < 0 -- Special case for db-sync restart rollback to epoch start
    ) THEN
      RAISE NOTICE 'Re-running...';
      CALL grest.update_pool_info_cache ();
    ELSE
      RAISE NOTICE 'Minimum block height difference(45) for update not reached, skipping...';
    END IF;

    RETURN;
END;
$$;

COMMENT ON FUNCTION grest.pool_info_cache_update_check IS 'Determines whether or not the pool info cache should be updated based ON the time rule (max once in 15 mins), and ensures previous run completed.';

CREATE OR REPLACE FUNCTION grest.pool_info_retire_status()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE grest.pool_info_cache
  SET pool_status = 'retired'
  WHERE pool_status = 'retiring'
    AND retiring_epoch <= new.no;
  RETURN NULL;
END;
$$;

COMMENT ON FUNCTION grest.pool_info_retire_status IS 'Internal function to update pool_info_cache with new retire status based ON epoch switch'; -- noqa: LT01

DROP TRIGGER IF EXISTS pool_info_retire_status_trigger ON public.epoch;

CREATE TRIGGER pool_info_retire_status_trigger
AFTER INSERT ON public.epoch
FOR EACH ROW EXECUTE FUNCTION grest.pool_info_retire_status();


CREATE INDEX IF NOT EXISTS idx_id ON grest.pool_info_cache (tx_id);
CREATE INDEX IF NOT EXISTS idx_tx_id ON grest.pool_info_cache (tx_id);
CREATE INDEX IF NOT EXISTS idx_pool_hash_id ON grest.pool_info_cache (pool_hash_id);
CREATE INDEX IF NOT EXISTS idx_pool_status ON grest.pool_info_cache (pool_status);
CREATE INDEX IF NOT EXISTS idx_meta_id ON grest.pool_info_cache (meta_id);
-- populated by first cron execution
