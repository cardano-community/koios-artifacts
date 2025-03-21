CREATE OR REPLACE FUNCTION grest.pool_calidus_keys()
RETURNS TABLE (
  pool_id_bech32 varchar,
  pool_status text,
  calidus_nonce bigint,
  calidus_pub_key text,
  calidus_id_bech32 text,
  tx_hash text,
  epoch_no word31type,
  block_height word31type,
  block_time integer
)
LANGUAGE SQL STABLE
AS $$
  SELECT DISTINCT ON (x.pool_id_bech32)
    x.pool_id_bech32,
    x.pool_status,
    x.calidus_nonce,
    x.calidus_key,
    cardano.bech32_encode('calidus', E'\\xa1' || cardano.blake2b_hash(DECODE(x.calidus_key, 'hex'), 28) ) AS calidus_id_bech32,
    x.tx_hash,
    x.epoch_no,
    x.block_height,
    x.block_time
  FROM (
    SELECT
      cardano.bech32_encode('pool', DECODE(SUBSTRING((tm.json->'1'->'1'->>1) FROM 3),'hex')) AS pool_id_bech32,
      (tm.json->'1'->>'4')::bigint AS calidus_nonce,
      SUBSTRING((tm.json->'1'->>'7') FROM 3) AS calidus_key,
      COALESCE(pic.pool_status,'unregistered') AS pool_status,
      cardano.tools_verify_cip88_pool_key_registration(tm.bytes) AS is_valid,
      ENCODE(tx.hash, 'hex') AS tx_hash,
      b.epoch_no AS epoch_no,
      b.block_no AS block_height,
      EXTRACT(EPOCH FROM b.time)::integer AS block_time
    FROM public.tx_metadata AS tm
      LEFT JOIN public.pool_hash AS ph ON ph.hash_raw = DECODE(SUBSTRING((tm.json->'1'->'1'->>1) FROM 3),'hex')
      LEFT JOIN grest.pool_info_cache AS pic ON pic.pool_hash_id = ph.id
      INNER JOIN public.tx ON tm.tx_id = tx.id
      INNER JOIN public.block AS b ON tx.block_id = b.id
    WHERE key=867
      AND (tm.json->>'0') IN ('2','3') -- Filter for records using CIP-0088 version 2 (and placeholder 3)
      AND (tm.json->'1'->'1'->>0) ='1' -- Filter for Pool ID registrations only
      AND (tm.json->'1'->'3'->>0) = '2' -- Ensure Signature validation method is CIP-0008
    ORDER BY tm.id DESC
  ) AS x
  WHERE is_valid=true
    AND length(calidus_key)=64
    AND calidus_key!='0000000000000000000000000000000000000000000000000000000000000000' -- De-registered key
  ORDER BY x.pool_id_bech32, x.calidus_nonce DESC;
$$;

COMMENT ON FUNCTION grest.pool_calidus_keys IS 'List of valid calidus keys for all pools'; --noqa: LT01
