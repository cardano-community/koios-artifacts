
CREATE OR REPLACE FUNCTION grest.is_dangling_delegation(delegation_id bigint)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
  curr_epoch bigint;
  num_retirements bigint;

BEGIN

SELECT INTO curr_epoch max(no) FROM epoch;

-- revised logic: 
-- check for any pool retirement record exists for the pool corresponding to given delegation
-- pool retiring epoch is current or in the past (future scheduled retirements don't count)
-- pool retiring epoch is after delegation cert submission epoch 
-- and there does not exist a pool_update transaction for this pool that came after currently analyzed pool retirement tx 
-- and before last transaction of the epoch preceeding the pool retirement epoch.. pool update submitted after that point in 
-- time is too late and pool should have been fully retired

SELECT INTO num_retirements count(*) 
FROM delegation d
  INNER JOIN pool_retire pr ON 
    d.id = delegation_id
    AND pr.hash_id = d.pool_hash_id
    AND pr.retiring_epoch <= curr_epoch 
    AND pr.retiring_epoch > (SELECT b.epoch_no FROM block b INNER JOIN tx t on t.id = d.tx_id and t.block_id = b.id)
    AND not exists 
      ( SELECT 1
        FROM pool_update pu
        WHERE pu.hash_id = d.pool_hash_id
          and pu.registered_tx_id >= pr.announced_tx_id
          and pu.registered_tx_id <= (SELECT i_last_tx_id 
                                      FROM grest.epoch_info_cache eic 
                                      WHERE eic.epoch_no = pr.retiring_epoch - 1)
      );

  return num_retirements > 0;
END;
$$;

COMMENT ON FUNCTION grest.is_dangling_delegation IS 'Returns a boolean to indicate whether a given delegation id corresponds to a delegation that has been made dangling by retirement of a stake pool associated with it'


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
              AND NOT grest.is_dangling_delegation(d.id)
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
