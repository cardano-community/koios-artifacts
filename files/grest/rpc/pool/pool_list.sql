CREATE OR REPLACE FUNCTION grest.pool_list ()
  RETURNS TABLE (
    pool_id_bech32 character varying,
    ticker character varying)
  LANGUAGE plpgsql
  AS $$
  # variable_conflict use_column
BEGIN

  RETURN QUERY (
    WITH
      -- Get last pool update for each pool
      _pool_list AS (
        SELECT
          DISTINCT ON (pic.pool_id_bech32) pool_id_bech32,
          pool_status
        FROM
          grest.pool_info_cache AS pic
        ORDER BY
          pic.pool_id_bech32,
          pic.tx_id DESC
      ),
      _pool_meta AS (
        SELECT
          DISTINCT ON (pic.pool_id_bech32) pool_id_bech32,
          pod.ticker_name
        FROM
          grest.pool_info_cache AS pic
          LEFT JOIN public.pool_offline_data AS pod ON pod.pmr_id = pic.meta_id
        WHERE pod.ticker_name IS NOT NULL
        ORDER BY
          pic.pool_id_bech32,
          pic.tx_id DESC
      )

    SELECT
      pl.pool_id_bech32,
      pm.ticker_name
    FROM
      _pool_list AS pl
      LEFT JOIN _pool_meta AS pm ON pl.pool_id_bech32 = pm.pool_id_bech32
    WHERE
      pool_status != 'retired'

  );

END;
$$;
