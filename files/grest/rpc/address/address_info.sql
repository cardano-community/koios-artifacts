CREATE OR REPLACE FUNCTION grest.address_info(_addresses text [])
RETURNS TABLE (
  address varchar,
  balance text,
  stake_address character varying,
  script_address boolean,
  utxo_set jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  known_addresses varchar[];
BEGIN
  CREATE TEMPORARY TABLE _known_addresses AS
    SELECT
      DISTINCT ON (tx_out.address) tx_out.address,
      sa.view AS stake_address,
      COALESCE(tx_out.address_has_script, 'false') AS script_address
    FROM tx_out
    LEFT JOIN stake_address AS sa ON sa.id = tx_out.stake_address_id
    WHERE tx_out.address = ANY(_addresses);

  RETURN QUERY
    WITH _all_utxos AS (
      SELECT
        tx.id,
        tx.hash,
        tx_out.id AS txo_id,
        tx_out.address,
        tx_out.value,
        tx_out.index,
        tx.block_id,
        tx_out.data_hash,
        tx_out.inline_datum_id,
        tx_out.reference_script_id
      FROM tx_out
      INNER JOIN tx ON tx.id = tx_out.tx_id
      WHERE tx_out.consumed_by_tx_in_id IS NULL
        AND tx_out.address = ANY(_addresses)
    )

    SELECT
      ka.address,
      COALESCE(SUM(au.value), '0')::text AS balance,
      ka.stake_address,
      ka.script_address,
      CASE
        WHEN EXISTS (
          SELECT TRUE FROM _all_utxos aus WHERE aus.address = ka.address
        ) THEN
          JSONB_AGG(
            JSONB_BUILD_OBJECT(
              'tx_hash', ENCODE(au.hash, 'hex'),
              'tx_index', au.index,
              'block_height', block.block_no,
              'block_time', EXTRACT(EPOCH FROM block.time)::integer,
              'value', au.value::text,
              'datum_hash', ENCODE(au.data_hash, 'hex'),
              'inline_datum',(
                CASE
                  WHEN au.inline_datum_id IS NULL THEN
                    NULL
                  ELSE
                    JSONB_BUILD_OBJECT(
                      'bytes', ENCODE(datum.bytes, 'hex'),
                      'value', datum.value
                    )
                END
              ),
              'reference_script',(
                CASE
                  WHEN au.reference_script_id IS NULL THEN
                    NULL
                  ELSE
                    JSONB_BUILD_OBJECT(
                      'hash', ENCODE(script.hash, 'hex'),
                      'bytes', ENCODE(script.bytes, 'hex'),
                      'value', script.json,
                      'type', script.type::text,
                      'size', script.serialised_size
                    )
                END
              ),
              'asset_list', COALESCE(
                (
                  SELECT
                    JSONB_AGG(JSONB_BUILD_OBJECT(
                      'policy_id', ENCODE(ma.policy, 'hex'),
                      'asset_name', ENCODE(ma.name, 'hex'),
                      'fingerprint', ma.fingerprint,
                      'decimals', COALESCE(aic.decimals, 0),
                      'quantity', mtx.quantity::text
                    ))
                  FROM ma_tx_out AS mtx
                  INNER JOIN multi_asset AS ma ON ma.id = mtx.ident
                  LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
                  WHERE mtx.tx_out_id = au.txo_id
                ),
                JSONB_BUILD_ARRAY()
              )
            )
          )
        ELSE
          '[]'::jsonb
        END AS utxo_set
      FROM _known_addresses AS ka
      LEFT OUTER JOIN _all_utxos AS au ON au.address = ka.address
      LEFT JOIN public.block ON block.id = au.block_id
      LEFT JOIN datum ON datum.id = au.inline_datum_id
      LEFT JOIN script ON script.id = au.reference_script_id
      GROUP BY
        ka.address,
        ka.stake_address,
        ka.script_address;
    DROP TABLE _known_addresses;
END;
$$;

COMMENT ON FUNCTION grest.address_info IS 'Get bulk address info - balance, associated stake address (if any) and UTXO set'; -- noqa: LT01
