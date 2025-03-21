CREATE OR REPLACE FUNCTION grest.pool_groups()
RETURNS TABLE (
  pool_id_bech32 text,
  pool_group text,
  ticker text,
  adastat_group text,
  balanceanalytics_group text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    pgrp.pool_id_bech32,
    pgrp.pool_group,
    pgrp.ticker,
    pgrp.adastat_group,
    pgrp.balanceanalytics_group
  FROM grest.pool_groups AS pgrp
  ORDER BY
    pgrp.pool_group,
    pgrp.ticker
  ;
$$;
