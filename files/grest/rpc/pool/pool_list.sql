CREATE OR REPLACE FUNCTION grest.pool_list()
RETURNS TABLE (
  pool_id_bech32 varchar,
  pool_id_hex text,
  active_epoch_no bigint,
  margin double precision,
  fixed_cost text,
  pledge text,
  deposit text,
  reward_addr varchar,
  owners varchar [],
  relays jsonb [],
  ticker varchar,
  pool_group text,
  meta_url varchar,
  meta_hash text,
  pool_status text,
  active_stake text,
  retiring_epoch word31type
)
LANGUAGE sql STABLE
AS $$
  SELECT DISTINCT ON (pic.pool_hash_id)
    cardano.bech32_encode('pool', ph.hash_raw)::varchar AS pool_id_bech32,
    ENCODE(ph.hash_raw,'hex') as pool_id_hex,
    pu.active_epoch_no,
    pu.margin,
    pu.fixed_cost::text,
    pu.pledge::text,
    pu.deposit::text,
    grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar AS reward_addr,
    ARRAY(
      SELECT grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar
      FROM public.pool_owner AS po
      INNER JOIN public.stake_address AS sa ON sa.id = po.addr_id
      WHERE po.pool_update_id = pic.update_id
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
      WHERE pr.update_id = pic.update_id
    ) AS relays,
    ocpd.ticker_name,
    pgrp.pool_group,
    pmr.url AS meta_url,
    pmr.hash AS meta_hash,
    pic.pool_status,
    pasc.amount::text AS active_stake,
    pic.retiring_epoch
  FROM grest.pool_info_cache AS pic
    INNER JOIN public.pool_hash AS ph ON ph.id = pic.pool_hash_id
    LEFT JOIN grest.pool_groups AS pgrp ON pgrp.pool_id_bech32 = ph.view
    LEFT JOIN grest.pool_active_stake_cache AS pasc ON pasc.pool_id = pic.pool_hash_id
    LEFT JOIN public.pool_update AS pu ON pu.id = pic.update_id
    LEFT JOIN public.stake_address AS sa ON pu.reward_addr_id = sa.id
    LEFT JOIN public.pool_metadata_ref AS pmr ON pmr.id = pic.meta_id
    LEFT JOIN public.off_chain_pool_data AS ocpd ON ocpd.pmr_id = pic.meta_id
  ORDER BY
    pic.pool_hash_id,
    pic.tx_id DESC,
    pasc.epoch_no DESC
  ;
$$;
