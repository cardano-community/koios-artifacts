CREATE OR REPLACE FUNCTION grest.pool_info(_pool_bech32_ids text [])
RETURNS TABLE (
  pool_id_bech32 varchar,
  pool_id_hex text,
  active_epoch_no bigint,
  vrf_key_hash text,
  margin double precision,
  fixed_cost text,
  pledge text,
  deposit text,
  reward_addr varchar,
  reward_addr_delegated_drep text,
  owners varchar [],
  relays jsonb [],
  meta_url varchar,
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
  live_saturation numeric,
  voting_power text
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
          cardano.bech32_encode('pool', ph.hash_raw) AS pool_id_bech32,
          ph.hash_raw
        FROM grest.pool_info_cache AS pic
        INNER JOIN public.pool_hash AS ph ON ph.id = pic.pool_hash_id
        -- consider only activated updates or all updates if none were activated so far
            AND ( (pic.active_epoch_no <= _epoch_no)
            OR ( NOT EXISTS (SELECT 1 from grest.pool_info_cache AS pic2 where pic2.pool_hash_id = pic.pool_hash_id
                AND pic2.active_epoch_no <= _epoch_no) ) )
        WHERE ph.hash_raw = ANY(
          SELECT cardano.bech32_decode_data(p)
          FROM UNNEST(_pool_bech32_ids) AS p)
        ORDER BY
          pic.pool_hash_id,
          pic.tx_id DESC
      )
    SELECT
      api.pool_id_bech32::varchar,
      ENCODE(ph.hash_raw::bytea, 'hex') AS pool_id_hex,
      pu.active_epoch_no,
      ENCODE(pu.vrf_key_hash, 'hex') AS vrf_key_hash,
      pu.margin,
      pu.fixed_cost::text,
      pu.pledge::text,
      pu.deposit::text,
      grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar AS reward_addr,
      COALESCE(grest.cip129_hex_to_drep_id(dh.raw, dh.has_script), dh.view::text) AS reward_addr_delegated_drep,
      ARRAY(
        SELECT grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar
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
      ROUND((live.stake / _saturation_limit) * 100, 2),
      pst.voting_power::text
    FROM _all_pool_info AS api
    LEFT JOIN public.pool_hash AS ph ON ph.id = api.pool_hash_id
    LEFT JOIN public.pool_update AS pu ON pu.id = api.update_id
    LEFT JOIN public.stake_address AS sa ON pu.reward_addr_id = sa.id
    LEFT JOIN delegation_vote AS dv on dv.addr_id = sa.id
	    AND NOT EXISTS (SELECT 1 FROM delegation_vote dv2 WHERE dv2.addr_id = sa.id AND dv2.tx_id > dv.tx_id)
	  LEFT JOIN drep_hash AS dh ON dh.id = dv.drep_hash_id
	        -- could add this condition too since delegations elsewhere are meaningless: and dh.view like 'drep_always%'
    LEFT JOIN public.pool_metadata_ref AS pmr ON pmr.id = api.meta_id
    LEFT JOIN public.off_chain_pool_data AS ocpd ON api.meta_id = ocpd.pmr_id
    LEFT JOIN public.pool_stat AS pst ON pst.pool_hash_id = api.pool_hash_id AND pst.epoch_no = _epoch_no
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
      WHERE pasc.pool_id = api.pool_hash_id
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
            WHEN cardano.bech32_decode_data(pool_delegs.stake_address) IN (
                SELECT sa.hash_raw
                FROM public.pool_owner AS po
                INNER JOIN public.stake_address AS sa ON sa.id = po.addr_id
                WHERE po.pool_update_id = api.update_id
              ) THEN amount::numeric
            ELSE 0
          END)::lovelace
        END AS pledge
      FROM grest.pool_delegators_list(api.pool_id_bech32) AS pool_delegs
    ) AS live ON TRUE;
END;
$$;

COMMENT ON FUNCTION grest.pool_info IS 'Current pool status and details for a specified list of pool ids'; --noqa: LT01
