CREATE OR REPLACE FUNCTION grest.pool_owner_history(_pool_bech32_ids text [])
RETURNS TABLE (
  pool_id_bech32 varchar,
  stake_address varchar,
  epoch_no bigint,
  declared_pledge text,
  active_stake text
)
LANGUAGE sql STABLE
AS $$
  SELECT DISTINCT ON (ph.view, es.epoch_no, sa.view)
    ph.view AS pool_id_bech32,
    sa.view AS stake_address,
    es.epoch_no::bigint,
    pu.pledge::text AS declared_pledge,
    SUM(es.amount)::text AS active_stake
  FROM pool_hash AS ph
    INNER JOIN pool_update AS pu ON pu.hash_id = ph.id
    -- only use last pool_update record for a given active_epoch_no in case multiple were submitted within one epoch
      AND NOT EXISTS (SELECT 1 FROM pool_update pu2 WHERE pu2.hash_id = pu.hash_id AND pu2.active_epoch_no = pu.active_epoch_no AND pu2.id > pu.id)
    INNER JOIN pool_owner AS po ON po.pool_update_id = pu.id
    INNER JOIN stake_address AS sa ON po.addr_id = sa.id
    INNER JOIN epoch_stake AS es ON po.addr_id = es.addr_id
      AND es.pool_id = ph.id
      -- only interested in active stake of owners between epoch in which they were activated
      -- via pool_update and the epoch before the next pool_update came into effect,
      -- or last populated epoch in epoch_stake if there haven't been further updates
      AND es.epoch_no BETWEEN pu.active_epoch_no AND
        (SELECT COALESCE(
            MIN(pu3.active_epoch_no) - 1,
            (SELECT MAX(epoch_no) FROM epoch_stake_progress where completed)
          )
          FROM pool_update pu3 WHERE pu3.hash_id = pu.hash_id AND pu3.id > pu.id)
  WHERE ph.id IN (SELECT id FROM pool_hash AS ph WHERE ph.hash_raw = ANY(
        SELECT cardano.bech32_decode_data(p)
        FROM UNNEST(_pool_bech32_ids) AS p))
  GROUP BY sa.view, ph.view, es.epoch_no, pu.id
  ORDER by ph.view, es.epoch_no DESC, sa.view;
$$;

COMMENT ON FUNCTION grest.pool_owner_history IS 'Pool Owners declared pledge v/s active stake history'; -- noqa: LT01
