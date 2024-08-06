CREATE OR REPLACE FUNCTION grest.pool_info(_pool_bech32_ids text [])
RETURNS TABLE (
  pool_id_bech32 character varying,
  pool_id_hex text,
  active_epoch_no bigint,
  vrf_key_hash text,
  margin double precision,
  fixed_cost text,
  pledge text,
  deposit text,
  reward_addr character varying,
  owners character varying [],
  relays jsonb [],
  meta_url character varying,
  meta_hash text,
  meta_json jsonb,
  pool_status text,
  retiring_epoch word31type,
  op_cert text,
  op_cert_counter word63type,
  active_stake text,
  sigma numeric,
  block_count numeric,
  live_pledge text,
  live_stake text,
  live_delegators bigint,
  live_saturation numeric
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
  _epoch_no bigint;
  _saturation_limit bigint;
BEGIN
  SELECT MAX(epoch.no) INTO _epoch_no FROM public.epoch;
  SELECT FLOOR(supply::bigint / (
      SELECT ep.optimal_pool_count
      FROM epoch_param AS ep
      WHERE ep.epoch_no = _epoch_no
    ))::bigint INTO _saturation_limit FROM grest.totals(_epoch_no);
  RETURN QUERY
    WITH
      _all_pool_info AS (
        SELECT DISTINCT ON (pic.pool_hash_id)
          pic.pool_hash_id,
          pic.active_epoch_no,
          pic.update_id,
          pic.pool_status,
          pic.retiring_epoch,
          pic.meta_id,
          ph.view,
          ph.hash_raw
        FROM grest.pool_info_cache AS pic
        INNER JOIN public.pool_hash AS ph ON ph.id = pic.pool_hash_id
        WHERE ph.view = ANY(SELECT UNNEST(_pool_bech32_ids))
        ORDER BY
          pic.pool_hash_id,
          pic.tx_id DESC
      )
    SELECT
      ph.view AS pool_id_bech32,
      ENCODE(ph.hash_raw::bytea, 'hex') AS pool_id_hex,
      pu.active_epoch_no,
      ENCODE(pu.vrf_key_hash, 'hex') AS vrf_key_hash,
      pu.margin,
      pu.fixed_cost::text,
      pu.pledge::text,
      pu.deposit::text,
      sa.view AS reward_addr,
      ARRAY(
        SELECT sa.view
        FROM public.pool_owner AS po
        INNER JOIN public.stake_address AS sa ON sa.id = po.addr_id
        WHERE po.pool_update_id = api.update_id
      ) AS owners,
      ARRAY(
        SELECT JSONB_BUILD_OBJECT(
          'ipv4', pr.ipv4,
          'ipv6', pr.ipv6,
          'dns', pr.dns_name,
          'srv', pr.dns_srv_name,
          'port', pr.port
        ) relay
        FROM public.pool_relay AS pr
        WHERE pr.update_id = api.update_id
      ) AS relays,
      pmr.url AS meta_url,
      ENCODE(pmr.hash,'hex') AS meta_hash,
      ocpd.json,
      api.pool_status,
      api.retiring_epoch,
      ENCODE(block_data.op_cert::bytea, 'hex'),
      block_data.op_cert_counter,
      active_stake.as_sum::text,
      active_stake.as_sum / epoch_stake.es_sum,
      block_data.cnt,
      live.pledge::text,
      live.stake::text,
      live.delegators,
      ROUND((live.stake / _saturation_limit) * 100, 2)
    FROM _all_pool_info AS api
    LEFT JOIN public.pool_hash AS ph ON ph.id = api.pool_hash_id
    LEFT JOIN public.pool_update AS pu ON pu.id = api.update_id
    LEFT JOIN public.stake_address AS sa ON pu.reward_addr_id = sa.id
    LEFT JOIN public.pool_metadata_ref AS pmr ON pmr.id = api.meta_id
    LEFT JOIN public.off_chain_pool_data AS ocpd ON api.meta_id = ocpd.pmr_id
    LEFT JOIN LATERAL (
      SELECT
        SUM(COUNT(b.id)) OVER () AS cnt,
        b.op_cert,
        b.op_cert_counter
      FROM public.block AS b
      INNER JOIN public.slot_leader AS sl ON b.slot_leader_id = sl.id
      WHERE sl.pool_hash_id = api.pool_hash_id
      GROUP BY
        b.op_cert,
        b.op_cert_counter
      ORDER BY b.op_cert_counter DESC
      LIMIT 1
    ) AS block_data ON TRUE
    LEFT JOIN LATERAL(
      SELECT amount::lovelace AS as_sum
      FROM grest.pool_active_stake_cache AS pasc
      WHERE pasc.pool_id = api.view
        AND pasc.epoch_no = _epoch_no
    ) AS active_stake ON TRUE
    LEFT JOIN LATERAL(
      SELECT amount::lovelace AS es_sum
      FROM grest.epoch_active_stake_cache AS easc
      WHERE easc.epoch_no = _epoch_no
    ) AS epoch_stake ON TRUE
    LEFT JOIN LATERAL(
      SELECT
        CASE WHEN api.pool_status = 'retired'
          THEN NULL
        ELSE
          SUM(
            CASE WHEN amount::numeric >= 0
              THEN amount::numeric
              ELSE 0
            END
          )::lovelace
        END AS stake,
        COUNT(stake_address) AS delegators,
        CASE WHEN api.pool_status = 'retired'
          THEN NULL
        ELSE
          SUM(CASE
            WHEN pool_delegs.stake_address IN (
                SELECT sa.view
                FROM public.pool_owner AS po
                INNER JOIN public.stake_address AS sa ON sa.id = po.addr_id
                WHERE po.pool_update_id = api.update_id
              ) THEN amount::numeric
            ELSE 0
          END)::lovelace
        END AS pledge
      FROM grest.pool_delegators_list(api.view) AS pool_delegs
    ) AS live ON TRUE;
END;
$$;

COMMENT ON FUNCTION grest.pool_info IS 'Current pool status and details for a specified list of pool ids'; --noqa: LT01
