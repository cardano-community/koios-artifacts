-- ACCOUNT

CREATE OR REPLACE FUNCTION grestv0.account_addresses(_stake_addresses text [], _first_only boolean DEFAULT FALSE, _empty boolean DEFAULT FALSE)
RETURNS TABLE (
  stake_address varchar,
  addresses jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.account_addresses(_stake_addresses,_first_only,_empty);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.account_assets(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  asset_list jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  sa_id_list integer[];
BEGIN
  SELECT INTO sa_id_list
    ARRAY_AGG(stake_address.id)
  FROM
    stake_address
  WHERE
    stake_address.view = ANY(_stake_addresses);
  RETURN QUERY
    WITH _all_assets AS (
      SELECT
        sa.view,
        ma.policy,
        ma.name,
        ma.fingerprint,
        COALESCE(aic.decimals, 0) AS decimals,
        SUM(mtx.quantity) AS quantity
      FROM
        ma_tx_out AS mtx
        INNER JOIN multi_asset AS ma ON ma.id = mtx.ident
        INNER JOIN tx_out AS txo ON txo.id = mtx.tx_out_id
        INNER JOIN stake_address AS sa ON sa.id = txo.stake_address_id
        LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
      WHERE sa.id = ANY(sa_id_list)
        AND txo.consumed_by_tx_in_id IS NULL
      GROUP BY
        sa.view, ma.policy, ma.name, ma.fingerprint, aic.decimals
    )

    SELECT
      assets_grouped.view AS stake_address,
      assets_grouped.assets
    FROM (
      SELECT
        aa.view,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'policy_id', ENCODE(aa.policy, 'hex'),
            'asset_name', ENCODE(aa.name, 'hex'),
            'fingerprint', aa.fingerprint,
            'decimals', COALESCE(aa.decimals, 0),
            'quantity', aa.quantity::text
          )
        ) AS assets
      FROM
        _all_assets AS aa
      GROUP BY
        aa.view
    ) AS assets_grouped;
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.account_history(_stake_addresses text [], _epoch_no integer DEFAULT NULL)
RETURNS TABLE (
  stake_address varchar,
  history jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.account_history(_stake_addresses, _epoch_no);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.account_info(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  status text,
  delegated_pool varchar,
  total_balance text,
  utxo text,
  rewards text,
  withdrawals text,
  rewards_available text,
  reserves text,
  treasury text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.account_info(_stake_addresses);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.account_info_cached(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  status text,
  delegated_pool varchar,
  total_balance text,
  utxo text,
  rewards text,
  withdrawals text,
  rewards_available text,
  reserves text,
  treasury text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.account_info_cached(_stake_addresses);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.account_rewards(_stake_addresses text [], _epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  stake_address varchar,
  rewards jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.account_rewards(_stake_addresses, _epoch_no);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.account_updates(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  updates jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.account_updates(_stake_addresses);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.account_utxos(_stake_address text)
RETURNS TABLE (
  tx_hash text,
  tx_index smallint,
  address varchar,
  value text,
  block_height word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT
    ENCODE(tx.hash,'hex') AS tx_hash,
    tx_out.index::smallint AS tx_index,
    tx_out.address,
    tx_out.value::text AS value,
    b.block_no AS block_height,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time
  FROM
    tx_out
    INNER JOIN tx ON tx.id = tx_out.tx_id
    LEFT JOIN block AS b ON b.id = tx.block_id
  WHERE tx_out.consumed_by_tx_in_id IS NULL
    AND tx_out.stake_address_id = (SELECT id FROM stake_address WHERE view = _stake_address);
END;
$$;
