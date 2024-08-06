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
  owners jsonb,
  relays jsonb,
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
        ENCODE(tx.hash::bytea, 'hex') AS tx_hash,
        EXTRACT(EPOCH FROM b.time)::integer AS block_time,
        ph.view AS pool_id_bech32,
        ENCODE(ph.hash_raw::bytea, 'hex') AS pool_id_hex,
        pu.active_epoch_no,
        ENCODE(pu.vrf_key_hash, 'hex') AS vrf_key_hash,
        pu.margin,
        pu.fixed_cost::text,
        pu.pledge::text,
        sa.view AS reward_addr,
        JSONB_AGG(po.view) AS owners,
        JSONB_AGG(JSONB_BUILD_OBJECT (
            'ipv4', pr.ipv4,
            'ipv6', pr.ipv6,
            'dns', pr.dns_name,
            'srv', pr.dns_srv_name,
            'port', pr.port
          )) AS relays,
        pmr.url AS meta_url,
        ENCODE(pmr.hash, 'hex') AS meta_hash,
        ocpd.json,
        'registration' AS update_type,
        NULL::word31type AS retiring_epoch
      FROM public.pool_hash AS ph
        LEFT JOIN public.pool_update AS pu ON pu.hash_id = ph.id
        INNER JOIN public.tx ON pu.registered_tx_id = tx.id
        INNER JOIN public.block AS b ON b.id = tx.block_id
        LEFT JOIN public.stake_address AS sa ON pu.reward_addr_id = sa.id
        LEFT JOIN (
            SELECT po1.pool_update_id, sa1.view
            FROM public.pool_owner AS po1
              LEFT JOIN public.stake_address AS sa1 ON sa1.id = po1.addr_id
          ) AS po ON pu.id = po.pool_update_id
        LEFT JOIN public.pool_relay AS pr ON pu.id = pr.update_id
        LEFT JOIN public.pool_metadata_ref AS pmr ON pu.meta_id = pmr.id
        LEFT JOIN public.off_chain_pool_data AS ocpd ON pu.meta_id = ocpd.pmr_id
      WHERE _pool_bech32 IS NULL
        OR ph.view = _pool_bech32
      GROUP BY tx.hash, b.time, ph.view, ph.hash_raw, pu.active_epoch_no, pu.vrf_key_hash, pu.margin, pu.fixed_cost, pu.pledge, sa.view, pmr.url, pmr.hash, ocpd.json),
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
          NULL::jsonb AS owners,
          NULL::jsonb AS relays,
          NULL AS meta_url,
          NULL AS meta_hash,
          NULL::jsonb AS json,
          'deregistration' AS update_type,
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
