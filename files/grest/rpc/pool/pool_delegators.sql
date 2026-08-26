CREATE OR REPLACE FUNCTION grest.pool_delegators(_pool_bech32 text)
RETURNS TABLE (
  stake_address varchar,
  amount text,
  active_epoch_no bigint,
  latest_delegation_tx_hash text
)
LANGUAGE sql STABLE
AS $$
  -- Converted from plpgsql to sql: removes plpgsql per-row overhead (variable lookups,
  -- RETURN QUERY handoff). Semantics preserved including the DISTINCT ON ... d.tx_id DESC
  -- which keeps the most recent delegation per stake_address.
  WITH _current_epoch AS (
    SELECT MAX(epoch_no) AS epoch_no FROM public.epoch_param
  ),
  _all_delegations AS (
    SELECT sa.id AS stake_address_id, sa.hash_raw AS stake_address_raw,
      (CASE WHEN sdc.total_balance >= 0 THEN sdc.total_balance ELSE 0 END) AS total_balance
    FROM grest.stake_distribution_cache AS sdc
    INNER JOIN public.stake_address AS sa ON sa.id = sdc.stake_address_id
    WHERE sdc.pool_id = (SELECT id FROM pool_hash WHERE pool_hash.hash_raw = cardano.bech32_decode_data(_pool_bech32))

    UNION ALL

    -- combine with registered delegations not in stake-dist-cache yet
    SELECT sa.id AS stake_address_id, sa.hash_raw AS stake_address_raw,
      COALESCE(SUM(acc_utxos.value::numeric), 0) AS total_balance
    FROM delegation AS d
    INNER JOIN stake_address AS sa ON d.addr_id = sa.id
    LEFT JOIN LATERAL
      grest.account_utxos(ARRAY[grest.cip5_hex_to_stake_addr(sa.hash_raw)], false) AS acc_utxos
      ON TRUE
    WHERE d.pool_hash_id = (SELECT id FROM pool_hash WHERE pool_hash.hash_raw = cardano.bech32_decode_data(_pool_bech32))
      AND NOT EXISTS (SELECT 1 FROM delegation AS d2 WHERE d2.addr_id = d.addr_id AND d2.id > d.id)
      AND NOT EXISTS (SELECT 1 FROM stake_deregistration AS sd WHERE sd.addr_id = d.addr_id AND sd.tx_id > d.tx_id)
      AND NOT EXISTS (SELECT 1 FROM grest.stake_distribution_cache AS sdc WHERE sdc.stake_address_id = sa.id)
      AND EXISTS (SELECT 1 FROM public.epoch_stake es WHERE es.addr_id = d.addr_id AND es.epoch_no = (SELECT epoch_no FROM _current_epoch))
    GROUP BY sa.id, sa.hash_raw
  )
  SELECT DISTINCT ON (ad.stake_address_raw)
    grest.cip5_hex_to_stake_addr(ad.stake_address_raw)::varchar,
    ad.total_balance::text,
    d.active_epoch_no,
    ENCODE(tx.hash, 'hex')
  FROM _all_delegations AS ad
  INNER JOIN public.delegation AS d ON d.addr_id = ad.stake_address_id
  INNER JOIN public.tx ON tx.id = d.tx_id
  ORDER BY ad.stake_address_raw, d.tx_id DESC;
$$;

COMMENT ON FUNCTION grest.pool_delegators IS 'Return information about live delegators for a given pool.'; --noqa: LT01

CREATE OR REPLACE FUNCTION grest.pool_delegators_list(_pool_bech32 text)
RETURNS TABLE (
  stake_address varchar,
  amount text
)
LANGUAGE sql STABLE
AS $$
  -- Converted from plpgsql to sql for the same per-row overhead reasons as pool_delegators
  WITH _current_epoch AS (
    SELECT MAX(epoch_no) AS epoch_no FROM public.epoch_param
  ),
  _all_delegations AS (
    SELECT sa.id AS stake_address_id, sa.hash_raw AS stake_address_raw,
      (CASE WHEN sdc.total_balance >= 0 THEN sdc.total_balance ELSE 0 END) AS total_balance
    FROM grest.stake_distribution_cache AS sdc
    INNER JOIN public.stake_address AS sa ON sa.id = sdc.stake_address_id
    WHERE sdc.pool_id = (SELECT id FROM pool_hash WHERE pool_hash.hash_raw = cardano.bech32_decode_data(_pool_bech32))

    UNION ALL

    SELECT sa.id AS stake_address_id, sa.hash_raw AS stake_address_raw,
      COALESCE(SUM(acc_utxos.value::numeric), 0) AS total_balance
    FROM delegation AS d
    INNER JOIN stake_address AS sa ON d.addr_id = sa.id
    LEFT JOIN LATERAL
      grest.account_utxos(ARRAY[grest.cip5_hex_to_stake_addr(sa.hash_raw)], false) AS acc_utxos
      ON TRUE
    WHERE d.pool_hash_id = (SELECT id FROM pool_hash WHERE pool_hash.hash_raw = cardano.bech32_decode_data(_pool_bech32))
      AND NOT EXISTS (SELECT 1 FROM delegation AS d2 WHERE d2.addr_id = d.addr_id AND d2.id > d.id)
      AND NOT EXISTS (SELECT 1 FROM stake_deregistration AS sd WHERE sd.addr_id = d.addr_id AND sd.tx_id > d.tx_id)
      AND NOT EXISTS (SELECT 1 FROM grest.stake_distribution_cache AS sdc WHERE sdc.stake_address_id = sa.id)
      AND EXISTS (SELECT 1 FROM public.epoch_stake es WHERE es.addr_id = d.addr_id AND es.epoch_no = (SELECT epoch_no FROM _current_epoch))
    GROUP BY sa.id, sa.hash_raw
  )
  SELECT
    grest.cip5_hex_to_stake_addr(ad.stake_address_raw)::varchar,
    ad.total_balance::text
  FROM _all_delegations AS ad;
$$;


COMMENT ON FUNCTION grest.pool_delegators_list IS 'Return brief variant of information about live delegators for a given pool, needed by pool_info endpoint.'; --noqa: LT01
