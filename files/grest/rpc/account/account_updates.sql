CREATE OR REPLACE FUNCTION grest.account_updates(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  updates jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  sa_id_list integer[] DEFAULT NULL;
BEGIN
  SELECT INTO sa_id_list
    ARRAY_AGG(stake_address.id)
  FROM
    stake_address
  WHERE
    stake_address.VIEW = ANY(_stake_addresses);

  RETURN QUERY
    SELECT
      sa.view AS stake_address,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'action_type', actions.action_type,
          'tx_hash', ENCODE(tx.hash, 'hex'),
          'epoch_no', b.epoch_no,
          'epoch_slot', b.epoch_slot_no,
          'absolute_slot', b.slot_no,
          'block_time', EXTRACT(EPOCH FROM b.time)::integer
        )
      )
    FROM (
      (
        SELECT
          'registration' AS action_type,
          tx_id,
          addr_id
        FROM
          stake_registration
        WHERE
          addr_id = ANY(sa_id_list)
      )
    UNION (
        SELECT
          'deregistration' AS action_type,
          tx_id,
          addr_id
        FROM
          stake_deregistration
        WHERE
          addr_id = ANY(sa_id_list)
      )
    UNION (
        SELECT
          'delegation' AS action_type,
          tx_id,
          addr_id
        FROM
          delegation
        WHERE
          addr_id = ANY(sa_id_list)
      )
    UNION (
        SELECT
          'withdrawal' AS action_type,
          tx_id,
          addr_id
        FROM
          withdrawal
        WHERE
          addr_id = ANY(sa_id_list)
      )
    ) AS actions
      INNER JOIN tx ON tx.id = actions.tx_id
      INNER JOIN stake_address AS sa ON sa.id = actions.addr_id
      INNER JOIN block AS b ON b.id = tx.block_id
    GROUP BY sa.id;
END;
$$;

COMMENT ON FUNCTION grest.account_updates IS 'Get updates (registration, deregistration, delegation and withdrawals) for given stake addresses'; -- noqa: LT01
