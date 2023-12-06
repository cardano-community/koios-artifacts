-- SCRIPT

CREATE OR REPLACE FUNCTION grestv0.datum_info(_datum_hashes text [])
RETURNS TABLE (
  hash text,
  value jsonb,
  bytes text
)
LANGUAGE plpgsql
AS $$
DECLARE
  _datum_hashes_decoded bytea[];
BEGIN
  SELECT INTO _datum_hashes_decoded ARRAY_AGG(DECODE(d_hash, 'hex'))
  FROM UNNEST(_datum_hashes) AS d_hash;
  RETURN QUERY
    SELECT
      ENCODE(d.hash, 'hex'),
      d.value,
      ENCODE(d.bytes, 'hex')
    FROM 
      datum AS d
    WHERE
      d.hash = ANY(_datum_hashes_decoded);
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.native_script_list()
RETURNS TABLE (
  script_hash text,
  creation_tx_hash text,
  type scripttype,
  script jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT
    ENCODE(script.hash, 'hex'),
    ENCODE(tx.hash, 'hex'),
    script.type,
    script.json
  FROM script
    INNER JOIN tx ON tx.id = script.tx_id
  WHERE script.type IN ('timelock', 'multisig');
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.plutus_script_list()
RETURNS TABLE (
  script_hash text,
  creation_tx_hash text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT
    ENCODE(script.hash, 'hex') AS script_hash,
    ENCODE(tx.hash, 'hex') AS creation_tx_hash
  FROM script
    INNER JOIN tx ON tx.id = script.tx_id
  WHERE script.type IN ('plutusV1', 'plutusV2');
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.script_redeemers(_script_hash text)
RETURNS TABLE (
  script_hash text,
  redeemers jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT * FROM grest.script_redeemers(_script_hash);
END;
$$;
