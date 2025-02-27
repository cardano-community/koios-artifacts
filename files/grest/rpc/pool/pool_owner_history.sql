CREATE OR REPLACE FUNCTION grest.pool_owner_history(_pool_bech32_ids text [])
RETURNS TABLE (
  pool_id_bech32 varchar,
  stake_address varchar,
  epoch_no bigint,
  declared_pledge text,
  active_stake text
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
  _curr_epoch word31type;
BEGIN

  SELECT MAX(no) INTO _curr_epoch FROM epoch;

  RETURN QUERY
    SELECT DISTINCT ON (ph.view, es.epoch_no, sa.view)
      ph.view AS pool_id_bech32,
      sa.view AS stake_address,
      es.epoch_no::bigint,
      pu.pledge::text AS declared_pledge,
      SUM(es.amount)::text AS active_stake
    FROM pool_hash AS ph
      INNER JOIN pool_update AS pu ON pu.hash_id = ph.id
      INNER JOIN tx ON pu.registered_tx_id = tx.id
      INNER JOIN block AS b ON b.id = tx.block_id
      INNER JOIN pool_owner AS po ON po.pool_update_id = pu.id
      INNER JOIN stake_address AS sa ON po.addr_id = sa.id
      INNER JOIN epoch_stake AS es ON po.addr_id = es.addr_id AND es.pool_id = ph.id AND es.epoch_no > b.epoch_no
    WHERE ph.id = (SELECT id FROM pool_hash AS ph WHERE ph.hash_raw = ANY(
          SELECT cardano.bech32_decode_data(p)
          FROM UNNEST(_pool_bech32_ids) AS p))
      AND pu.pledge != 0
      AND es.amount != 0
    GROUP BY sa.view, ph.view, es.epoch_no, b.epoch_no, pu.id
    ORDER by ph.view, es.epoch_no DESC, sa.view, b.epoch_no DESC;
END;
$$;

COMMENT ON FUNCTION grest.pool_owner_history IS 'Pool Owners declared pledge v/s active stake history'; -- noqa: LT01
