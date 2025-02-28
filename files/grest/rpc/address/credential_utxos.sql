CREATE OR REPLACE FUNCTION grest.credential_utxos(_payment_credentials text [], _extended boolean DEFAULT false)
RETURNS TABLE (
  tx_hash text,
  tx_index smallint,
  address text,
  value text,
  stake_address text,
  payment_cred text,
  epoch_no word31type,
  block_height word31type,
  block_time integer,
  datum_hash text,
  inline_datum jsonb,
  reference_script jsonb,
  asset_list jsonb,
  is_spent boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
  _payment_cred_bytea  bytea[];
BEGIN
  SELECT INTO _payment_cred_bytea ARRAY_AGG(cred_bytea)
  FROM (
    SELECT DECODE(cred_hex, 'hex') AS cred_bytea
    FROM UNNEST(_payment_credentials) AS cred_hex
  ) AS tmp;

  RETURN QUERY
    SELECT
      ENCODE(tx.hash, 'hex')::text AS tx_hash,
      tx_out.index::smallint,
      a.address::text,
      tx_out.value::text,
      sa.view::text AS stake_address,
      ENCODE(a.payment_cred, 'hex') AS payment_cred,
      b.epoch_no,
      b.block_no,
      EXTRACT(EPOCH FROM b.time)::integer AS block_time,
      ENCODE(tx_out.data_hash, 'hex') AS datum_hash,
      (CASE
        WHEN _extended = false OR tx_out.inline_datum_id IS NULL THEN NULL
        ELSE JSONB_BUILD_OBJECT(
            'bytes', ENCODE(datum.bytes, 'hex'),
            'value', datum.value
          )
      END) AS inline_datum,
      (CASE
        WHEN _extended = false OR tx_out.reference_script_id IS NULL THEN NULL
        ELSE JSONB_BUILD_OBJECT(
            'hash', ENCODE(script.hash, 'hex'),
            'bytes', ENCODE(script.bytes, 'hex'),
            'value', script.json,
            'type', script.type::text,
            'size', script.serialised_size
          )
      END) AS reference_script,
      (CASE
        WHEN _extended = false THEN NULL
        ELSE COALESCE(
          (
            SELECT
              JSONB_AGG(CASE
                WHEN ma.policy IS NULL THEN NULL
                ELSE JSONB_BUILD_OBJECT(
                  'policy_id', ENCODE(ma.policy, 'hex'),
                  'asset_name', ENCODE(ma.name, 'hex'),
                  'fingerprint', ma.fingerprint,
                  'decimals', aic.decimals,
                  'quantity', mto.quantity::text
                )
              END) AS assets
            FROM ma_tx_out AS mto
            INNER JOIN multi_asset AS ma ON mto.tx_out_id = tx_out.id and ma.id = mto.ident
            LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
            GROUP BY tx_out.id
          ), JSONB_BUILD_ARRAY())
      END) AS asset_list,
      false AS is_spent
    FROM tx_out
    INNER JOIN address AS a ON a.id = tx_out.address_id
    INNER JOIN tx ON tx_out.tx_id = tx.id
    LEFT JOIN stake_address AS sa ON tx_out.stake_address_id = sa.id
    LEFT JOIN block AS b ON b.id = tx.block_id
    LEFT JOIN datum ON datum.id = tx_out.inline_datum_id
    LEFT JOIN script ON script.id = tx_out.reference_script_id
    WHERE a.payment_cred = ANY(_payment_cred_bytea)
      AND tx_out.consumed_by_tx_id IS NULL
  ;
END;
$$;

COMMENT ON FUNCTION grest.credential_utxos IS  'Get UTxO details for requested credentials'; -- noqa: LT01
