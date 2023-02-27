CREATE OR REPLACE FUNCTION grest.credential_utxos (_payment_credentials text[])
  RETURNS TABLE (
    tx_hash text,
    tx_index smallint,
    value text
  )
  LANGUAGE PLPGSQL
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
      ENCODE(tx.hash, 'hex')::text as tx_hash,
      tx_out.index::smallint,
      tx_out.value::text AS balance
    FROM tx_out
      INNER JOIN tx ON tx_out.tx_id = tx.id
      LEFT JOIN tx_in ON tx_out.tx_id = tx_in.tx_out_id
        AND tx_out.index = tx_in.tx_out_index
    WHERE
      payment_cred = any(_payment_cred_bytea)
      AND
        tx_in.tx_out_id IS NULL;
END;
$$;
