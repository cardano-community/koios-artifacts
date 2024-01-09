CREATE OR REPLACE FUNCTION grest.script_utxos(_script_hash text, _extended boolean DEFAULT false)
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
  known_addresses varchar[];
BEGIN
  RETURN QUERY
    SELECT
      ENCODE(tx.hash, 'hex')::text AS tx_hash,
      tx_out.index::smallint,
      tx_out.address::text,
      tx_out.value::text,
      sa.view::text as stake_address,
      ENCODE(tx_out.payment_cred, 'hex') AS payment_cred,
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
        WHEN _extended = false OR ma.policy IS NULL THEN NULL
        ELSE JSONB_BUILD_OBJECT(
            'policy_id', ENCODE(ma.policy, 'hex'),
            'asset_name', ENCODE(ma.name, 'hex'),
            'fingerprint', ma.fingerprint,
            'decimals', aic.decimals,
            'quantity', mto.quantity::text
          )
        END
      ) AS asset_list,
      (CASE
        WHEN tx_out.consumed_by_tx_in_id IS NULL THEN false
        ELSE true
      END) AS is_spent
    FROM tx_out
    INNER JOIN tx ON tx_out.tx_id = tx.id
    INNER JOIN script ON script.tx_id = tx.id
    LEFT JOIN stake_address AS sa ON tx_out.stake_address_id = sa.id
    LEFT JOIN block AS b ON b.id = tx.block_id
    LEFT JOIN ma_tx_out AS mto ON mto.tx_out_id = tx_out.id
    LEFT JOIN multi_asset AS ma ON ma.id = mto.ident
    LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
    LEFT JOIN datum ON datum.id = tx_out.inline_datum_id
    WHERE script.hash = DECODE(_script_hash,'hex')
      AND tx_out.consumed_by_tx_in_id IS NULL
  ;
END;
$$;

COMMENT ON FUNCTION grest.script_utxos IS  'Get UTxO details for requested scripts'; -- noqa: LT01
