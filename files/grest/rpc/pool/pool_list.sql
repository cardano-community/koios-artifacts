CREATE OR REPLACE FUNCTION grest.pool_list()
RETURNS TABLE (
  pool_id_bech32 character varying,
  pool_id_hex text,
  active_epoch_no bigint,
  margin double precision,
  fixed_cost text,
  pledge text,
  reward_addr character varying,
  owners character varying [],
  relays jsonb [],
  ticker character varying,
  meta_url character varying,
  meta_hash text,
  pool_status text,
  retiring_epoch word31type
)
LANGUAGE plpgsql
AS $$
# variable_conflict use_column
BEGIN
  RETURN QUERY (
    WITH
      -- Get last pool update for each pool
      _pool_list AS (
        SELECT
          ph.view as pool_id_bech32,
          ph.hash_raw as pool_id_hex
        FROM pool_hash AS ph
      ),

      _pool_meta AS (
        SELECT DISTINCT ON (pic.pool_id_bech32)
          pic.pool_id_bech32,
          pic.active_epoch_no,
          pic.margin,
          pic.fixed_cost,
          pic.pledge,
          pic.reward_addr,
          pic.owners,
          pic.relays,
          ocpd.ticker_name,
          pic.meta_url,
          pic.meta_hash,
          pic.pool_status,
          pic.retiring_epoch
        FROM grest.pool_info_cache AS pic
        LEFT JOIN public.off_chain_pool_data AS ocpd ON ocpd.pmr_id = pic.meta_id
        ORDER BY
          pic.pool_id_bech32,
          pic.tx_id DESC
      )
    SELECT
      pl.pool_id_bech32,
      encode(pl.pool_id_hex,'hex') as pool_id_hex,
      pm.active_epoch_no,
      pm.margin,
      pm.fixed_cost::text,
      pm.pledge::text,
      pm.reward_addr,
      pm.owners,
      pm.relays,
      pm.ticker_name,
      pm.meta_url,
      pm.meta_hash,
      pm.pool_status,
      pm.retiring_epoch
    FROM _pool_list AS pl
    LEFT JOIN _pool_meta AS pm ON pl.pool_id_bech32 = pm.pool_id_bech32
  );
END;
$$;
