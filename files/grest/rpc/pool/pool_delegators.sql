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
        WHERE
          sdc.pool_id = _pool_bech32
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
      ad.stake_address, d.tx_id DESC;
END;
$$;

COMMENT ON FUNCTION grest.pool_delegators IS 'Return information about live delegators for a given pool.'; --noqa: LT01
