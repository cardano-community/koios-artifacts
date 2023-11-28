-- ADDRESS

CREATE OR REPLACE FUNCTION grestv0.address_assets(_addresses text [])
RETURNS TABLE (
  address varchar,
  asset_list jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY

  WITH _all_assets AS (
    SELECT
      txo.address,
      ma.policy,
      ma.name,
      ma.fingerprint,
      COALESCE(aic.decimals, 0) AS decimals,
      SUM(mtx.quantity) AS quantity
    FROM
      ma_tx_out AS mtx
      INNER JOIN multi_asset AS ma ON ma.id = mtx.ident
      LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
      INNER JOIN tx_out AS txo ON txo.id = mtx.tx_out_id
    WHERE
      txo.address = ANY(_addresses)
      AND txo.consumed_by_tx_in_id IS NULL
    GROUP BY
      txo.address, ma.policy, ma.name, ma.fingerprint, aic.decimals
  )

  SELECT
    assets_grouped.address,
    assets_grouped.asset_list
  FROM (
    SELECT
      aa.address,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'policy_id', ENCODE(aa.policy, 'hex'),
          'asset_name', ENCODE(aa.name, 'hex'),
          'fingerprint', aa.fingerprint,
          'decimals', aa.decimals,
          'quantity', aa.quantity::text
        )
      ) AS asset_list
    FROM 
      _all_assets AS aa
    GROUP BY
      aa.address
  ) assets_grouped;
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.address_info(_addresses text [])
RETURNS TABLE (
  address varchar,
  balance text,
  stake_address character varying,
  script_address boolean,
  utxo_set jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.address_info(_addresses);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.address_txs(_addresses text [], _after_block_height integer DEFAULT 0)
RETURNS TABLE (
  tx_hash text,
  epoch_no word31type,
  block_height word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.address_txs(_addresses, _after_block_height);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.credential_txs(_payment_credentials text [], _after_block_height integer DEFAULT 0)
RETURNS TABLE (
  tx_hash text,
  epoch_no word31type,
  block_height word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.credential_txs(_payment_credentials,_after_block_height);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.credential_utxos(_payment_credentials text [])
RETURNS TABLE (
  tx_hash text,
  tx_index smallint,
  value text
)
LANGUAGE plpgsql
AS $$
DECLARE
  _payment_cred_bytea  bytea[];

BEGIN
  SELECT INTO _payment_cred_bytea ARRAY_AGG(cred_bytea)
  FROM (
    SELECT
      DECODE(cred_hex, 'hex') AS cred_bytea
    FROM
      UNNEST(_payment_credentials) AS cred_hex
  ) AS tmp;

  RETURN QUERY
    SELECT
      ENCODE(tx.hash, 'hex')::text AS tx_hash,
      tx_out.index::smallint,
      tx_out.value::text AS balance
    FROM tx_out
      INNER JOIN tx ON tx_out.tx_id = tx.id
    WHERE payment_cred = ANY(_payment_cred_bytea)
      AND tx_out.consumed_by_tx_in_id IS NULL;
END;
$$;
