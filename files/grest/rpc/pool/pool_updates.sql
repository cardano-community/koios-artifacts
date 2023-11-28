CREATE OR REPLACE FUNCTION grest.pool_updates(_pool_bech32 text DEFAULT NULL)
RETURNS TABLE (
  tx_hash text,
  block_time integer,
  pool_id_bech32 character varying,
  pool_id_hex text,
  active_epoch_no bigint,
  vrf_key_hash text,
  margin double precision,
  fixed_cost text,
  pledge text,
  reward_addr character varying,
  owners character varying [],
  relays jsonb [],
  meta_url character varying,
  meta_hash text,
  meta_json jsonb,
  update_type text,
  retiring_epoch word31type
)
LANGUAGE plpgsql
AS $$
DECLARE
  _current_epoch_no word31type;
BEGIN
  SELECT COALESCE(MAX(no), 0) INTO _current_epoch_no FROM public.epoch;
  RETURN QUERY
  WITH
    pool_reg AS (
      SELECT
        pic.tx_hash,
        pic.block_time::integer,
        pic.pool_id_bech32,
        pic.pool_id_hex,
        pic.active_epoch_no,
        pic.vrf_key_hash,
        pic.margin,
        pic.fixed_cost::text,
        pic.pledge::text,
        pic.reward_addr,
        pic.owners,
        pic.relays,
        pic.meta_url,
        pic.meta_hash,
        pod.json,
        'registration' AS update_type,
        NULL::word31type AS retiring_epoch
      FROM
        grest.pool_info_cache AS pic
        LEFT JOIN public.pool_offline_data AS pod ON pod.pmr_id = pic.meta_id
        LEFT JOIN public.pool_retire AS pr ON pic.pool_hash_id = pr.hash_id
      WHERE _pool_bech32 IS NULL
        OR pic.pool_id_bech32 = _pool_bech32),
    pool_dereg AS (
        SELECT
          ENCODE(tx.hash::bytea, 'hex') AS tx_hash,
          EXTRACT(EPOCH FROM b.time)::integer AS block_time,
          ph.view AS pool_id_bech32,
          ENCODE(ph.hash_raw::bytea, 'hex') AS pool_id_hex,
          NULL::bigint AS active_epoch_no,
          NULL AS vrf_key_hash,
          NULL::bigint AS margin,
          NULL as fixed_cost,
          NULL AS pledge,
          NULL AS reward_addr,
          NULL::text[] AS owners,
          NULL::jsonb[] AS relays,
          NULL AS meta_url,
          NULL AS meta_hash,
          NULL::jsonb AS json,
          CASE
            WHEN pr.retiring_epoch IS NULL THEN 'registration'
            ELSE 'deregistration'
          END AS update_type,
          pr.retiring_epoch::word31type
        FROM public.pool_hash AS ph
          LEFT JOIN pool_retire AS pr ON pr.hash_id = ph.id
          INNER JOIN public.tx ON tx.id = pr.announced_tx_id
          INNER JOIN public.block AS b ON b.id = tx.block_id
          WHERE _pool_bech32 IS NULL
            OR ph.view = _pool_bech32)
  SELECT * FROM pool_reg
    UNION SELECT * FROM pool_dereg
  ORDER BY
    block_time DESC;
END;
$$;

COMMENT ON FUNCTION grest.pool_updates IS 'Return all pool_updates for all pools or only updates for specific pool if specified'; -- noqa: LT01
