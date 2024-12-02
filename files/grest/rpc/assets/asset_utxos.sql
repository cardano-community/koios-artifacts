CREATE OR REPLACE FUNCTION grest.asset_utxos(_asset_list text [] [], _extended boolean DEFAULT false)
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
  _asset_id_list bigint[];
BEGIN
  -- find all asset id's based ON nested array input
  SELECT INTO _asset_id_list ARRAY_AGG(id)
  FROM (
    SELECT DISTINCT mu.id
    FROM (
      SELECT
        DECODE(al->>0, 'hex') AS policy,
        DECODE(al->>1, 'hex') AS name
      FROM JSONB_ARRAY_ELEMENTS(TO_JSONB(_asset_list)) AS al
    ) AS ald
    INNER JOIN multi_asset AS mu ON mu.policy = ald.policy AND mu.name = ald.name
  ) AS tmp;

  RETURN QUERY
    WITH
      _txo_list AS (
        SELECT
          DISTINCT txo.id
        FROM tx_out AS txo
        INNER JOIN ma_tx_out AS mto ON mto.tx_out_id = txo.id
        WHERE mto.ident = ANY(_asset_id_list)
          AND txo.consumed_by_tx_id IS NULL
      )
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
      CASE
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
            INNER JOIN multi_asset AS ma ON mto.tx_out_id = my_txo_list.id and ma.id = mto.ident
            left JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
            GROUP BY my_txo_list.id
          ), JSONB_BUILD_ARRAY())
      END AS asset_list,
      false AS is_spent
    FROM _txo_list AS my_txo_list
    INNER JOIN tx_out on tx_out.id = my_txo_list.id
    INNER JOIN tx ON tx_out.tx_id = tx.id
    INNER JOIN address AS a ON a.id = tx_out.address_id
    LEFT JOIN stake_address AS sa ON tx_out.stake_address_id = sa.id
    LEFT JOIN block AS b ON b.id = tx.block_id
    LEFT JOIN datum ON datum.id = tx_out.inline_datum_id
    LEFT JOIN script ON script.id = tx_out.reference_script_id
    WHERE tx_out.consumed_by_tx_id IS NULL
  ;
END;
$$;

COMMENT ON FUNCTION grest.asset_utxos IS 'Get UTxO details for requested assets'; -- noqa: LT01
