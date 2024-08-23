CREATE OR REPLACE FUNCTION grest.drep_delegators(_drep_id text)
RETURNS TABLE (
  stake_address text,
  stake_address_hex text,
  script_hash text,
  epoch_no word31type,
  amount text
)
LANGUAGE plpgsql
AS $$
DECLARE
  drep_idx        bigint;
  last_reg_tx_id  bigint;
BEGIN

  SELECT INTO drep_idx id
  FROM public.drep_hash
  WHERE view = _drep_id;

  IF STARTS_WITH(_drep_id,'drep_') THEN
    -- predefined DRep roles
    last_reg_tx_id := 0;
  ELSE
    SELECT INTO last_reg_tx_id MAX(tx_id)
    FROM public.drep_registration
    WHERE drep_hash_id = drep_idx
      AND (deposit IS NOT NULL AND deposit >= 0);

    IF last_reg_tx_id IS NULL OR EXISTS (
      SELECT 1
      FROM public.drep_registration
      WHERE drep_hash_id = drep_idx
        AND deposit IS NOT NULL
        AND deposit < 0
        AND tx_id > last_reg_tx_id
      LIMIT 1
    ) THEN
      RETURN; -- DRep not registered or de-registered, no need to continue
    END IF;
  END IF;

  RETURN QUERY (
    WITH
      _all_delegations AS (
        SELECT *
        FROM (
          SELECT
            DISTINCT ON (last_delegation.addr_id) last_delegation.addr_id,
            last_delegation.tx_id,
            last_delegation.drep_hash_id,
            sd.tx_id AS dereg_tx_id
          FROM (
            SELECT
              DISTINCT ON (dv1.addr_id) dv1.addr_id,
              dv1.tx_id,
              dv1.drep_hash_id
            FROM
              public.delegation_vote AS dv1
            WHERE
              dv1.addr_id = ANY(
                SELECT dv2.addr_id
                FROM public.delegation_vote AS dv2
                WHERE dv2.drep_hash_id = drep_idx
                  AND dv2.tx_id >= last_reg_tx_id
              )
            ORDER BY
              dv1.addr_id, dv1.tx_id DESC
          ) AS last_delegation
          LEFT JOIN stake_deregistration AS sd ON last_delegation.addr_id = sd.addr_id AND last_delegation.tx_id < sd.tx_id
          WHERE last_delegation.drep_hash_id = drep_idx
          ORDER BY
            last_delegation.addr_id, sd.tx_id NULLS LAST
        ) AS all_delegations_w_dereg
        WHERE all_delegations_w_dereg.dereg_tx_id IS NULL
      )

    SELECT
      sa.view::text,
      ENCODE(sa.hash_raw,'hex'),
      ENCODE(sa.script_hash,'hex'),
      b.epoch_no,
      COALESCE(sdc.total_balance,0)::text
    FROM _all_delegations AS ad
      INNER JOIN stake_address AS sa ON ad.addr_id = sa.id
      INNER JOIN tx ON ad.tx_id = tx.id
      INNER JOIN block AS b ON tx.block_id = b.id
      LEFT JOIN grest.stake_distribution_cache AS sdc ON sa.view = sdc.stake_address
    ORDER BY b.epoch_no DESC, sa.view
  );

END;
$$;

COMMENT ON FUNCTION grest.drep_delegators IS 'Return all delegators for a specific DRep'; -- noqa: LT01
