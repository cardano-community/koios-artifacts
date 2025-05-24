CREATE OR REPLACE FUNCTION grest.tx_treasury_donations_epoch(_epoch_no numeric)
RETURNS TABLE (
  epoch_no word31type,
  tx_hash text,
  block_hash text,
  block_index smallint,
  block_height word31type,
  block_time integer,
  from_addresses jsonb,
  treasury_donation lovelace
)
LANGUAGE sql STABLE
AS $$
  SELECT
    b.epoch_no,
    ENCODE(tx.hash, 'hex')::text AS tx_hash,
    ENCODE(b.hash, 'hex')::text AS block_hash,
    tx.block_index::smallint AS block_index,
    b.block_no AS block_height,
    EXTRACT(EPOCH FROM b.time)::integer AS block_time,
    COALESCE(JSONB_AGG(
       JSONB_BUILD_OBJECT(
          'payment_addr', JSONB_BUILD_OBJECT(
            'bech32', a.address,
            'cred', ENCODE(a.payment_cred, 'hex')
          ),
          'stake_addr', grest.cip5_hex_to_stake_addr(sa.hash_raw)
      )
    ), JSONB_BUILD_ARRAY()) as from_addresses,
    tx.treasury_donation
  FROM public.tx AS tx
    INNER JOIN public.block AS b ON b.id = tx.block_id
    INNER JOIN public.tx_out AS txo ON txo.consumed_by_tx_id = tx.id
    INNER JOIN public.address AS a ON a.id = txo.address_id
    LEFT JOIN stake_address AS sa ON sa.id = txo.stake_address_id
  WHERE b.epoch_no = _epoch_no::word31type
    AND tx.treasury_donation > 0
  GROUP BY tx.id, tx.block_index, b.block_no, b.epoch_no, b.hash, b.time
  ORDER BY tx.id ASC;
$$;

COMMENT ON FUNCTION grest.tx_treasury_donations_epoch IS 'Get a list of treasury donation transactions for a given epoch'; -- noqa: LT01
