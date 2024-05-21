CREATE OR REPLACE FUNCTION grest.pool_delegators(_pool_bech32 text)
RETURNS TABLE (
  stake_address character varying,
  amount text,
  active_epoch_no bigint,
  latest_delegation_tx_hash text
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
  _pool_id bigint;
BEGIN
  RETURN QUERY
    WITH
      _all_delegations AS (
        SELECT
          sa.id AS stake_address_id,
          sdc.stake_address,
          (
            CASE WHEN sdc.total_balance >= 0
              THEN sdc.total_balance
              ELSE 0
            END
          ) AS total_balance
        FROM grest.stake_distribution_cache AS sdc
        INNER JOIN public.stake_address AS sa ON sa.view = sdc.stake_address
        WHERE sdc.pool_id = _pool_bech32

        UNION ALL

        -- combine with registered delegations not in stake-dist-cache yet
        SELECT 
          z.stake_address_id, z.stake_address, SUM(acc_info.value::numeric) AS total_balance
        FROM
          ( 
            SELECT
              sa.id AS stake_address_id,
              sa.view AS stake_address
            FROM delegation AS d 
	            INNER JOIN pool_hash AS ph ON d.pool_hash_id = ph.id AND ph.view = _pool_bech32
              INNER JOIN stake_address AS sa ON d.addr_id = sa.id
              AND NOT EXISTS (SELECT null FROM delegation AS d2 WHERE d2.addr_id = d.addr_id AND d2.id > d.id)
              AND NOT EXISTS (SELECT null FROM stake_deregistration AS sd WHERE sd.addr_id = d.addr_id AND sd.tx_id > d.tx_id)
              -- AND NOT grest.is_dangling_delegation(d.id)
              AND NOT EXISTS (SELECT null FROM grest.stake_distribution_cache AS sdc WHERE sdc.stake_address = sa.view)
          ) z,
          LATERAL grest.account_utxos(array[z.stake_address], false) AS acc_info
        GROUP BY
          z.stake_address_id,
          z.stake_address
      )

    SELECT DISTINCT ON (ad.stake_address)
      ad.stake_address,
      ad.total_balance::text,
      d.active_epoch_no,
      ENCODE(tx.hash, 'hex')
    FROM _all_delegations AS ad
    INNER JOIN public.delegation AS d ON d.addr_id = ad.stake_address_id
    INNER JOIN public.tx ON tx.id = d.tx_id
    ORDER BY
      ad.stake_address,
      d.tx_id DESC;
END;
$$;

COMMENT ON FUNCTION grest.pool_delegators IS 'Return information about live delegators for a given pool.'; --noqa: LT01




CREATE OR REPLACE FUNCTION grest.pool_delegators_list(_pool_bech32 text)
RETURNS TABLE (
  stake_address character varying,
  amount text
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
  _pool_id bigint;
BEGIN
  SELECT id INTO _pool_id FROM pool_hash WHERE pool_hash.view = _pool_bech32;

  RETURN QUERY
    WITH
      _all_delegations AS (
        SELECT
          sa.id AS stake_address_id,
          sdc.stake_address,
          (
            CASE WHEN sdc.total_balance >= 0
              THEN sdc.total_balance
              ELSE 0
            END
          ) AS total_balance
        FROM grest.stake_distribution_cache AS sdc
        INNER JOIN public.stake_address AS sa ON sa.view = sdc.stake_address
        WHERE sdc.pool_id = _pool_bech32

        UNION ALL

        -- combine with registered delegations not in stake-dist-cache yet
        SELECT 
          z.stake_address_id, z.stake_address, SUM(acc_info.value::numeric) AS total_balance
        FROM
          ( 
            SELECT
              sa.id AS stake_address_id,
              sa.view AS stake_address
            FROM delegation AS d 
              INNER JOIN stake_address AS sa ON d.addr_id = sa.id and d.pool_hash_id = _pool_id
              AND NOT EXISTS (SELECT null FROM delegation AS d2 WHERE d2.addr_id = d.addr_id AND d2.id > d.id)
              AND NOT EXISTS (SELECT null FROM stake_deregistration AS sd WHERE sd.addr_id = d.addr_id AND sd.tx_id > d.tx_id)
              -- AND NOT grest.is_dangling_delegation(d.id)
              AND NOT EXISTS (SELECT null FROM grest.stake_distribution_cache AS sdc WHERE sdc.stake_address = sa.view)
          ) z,
          LATERAL grest.account_utxos(array[z.stake_address], false) AS acc_info
        GROUP BY
          z.stake_address_id,
          z.stake_address
      )

    SELECT 
      ad.stake_address,
      ad.total_balance::text
    FROM _all_delegations AS ad;

END;
$$;


COMMENT ON FUNCTION grest.pool_delegators_list IS 'Return brief variant of information about live delegators for a given pool, needed by pool_info endpoint.'; --noqa: LT01
