CREATE OR REPLACE FUNCTION grest.pool_relays()
RETURNS TABLE (
  pool_id_bech32 varchar,
  relays jsonb
)
LANGUAGE sql STABLE
AS $$
  SELECT DISTINCT ON (ph.id)
    cardano.bech32_encode('pool', ph.hash_raw)::varchar AS pool_id_bech32,
    JSONB_AGG(JSONB_BUILD_OBJECT (
        'ipv4', pr.ipv4,
        'ipv6', pr.ipv6,
        'dns', pr.dns_name,
        'srv', pr.dns_srv_name,
        'port', pr.port
      )) AS relays
  FROM public.pool_hash AS ph
    LEFT JOIN public.pool_update AS pu ON pu.hash_id = ph.id
    LEFT JOIN public.pool_relay AS pr ON pu.id = pr.update_id
  GROUP BY ph.id, ph.hash_raw,pu.registered_tx_id
  ORDER BY
    ph.id,
    pu.registered_tx_id DESC
  ;
$$;

COMMENT ON FUNCTION grest.pool_relays IS 'A list of registered relays for all pools'; --noqa: LT01
