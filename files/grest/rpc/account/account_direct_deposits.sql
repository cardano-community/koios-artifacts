CREATE OR REPLACE FUNCTION grest.account_direct_deposits(_stake_addresses text [])
RETURNS TABLE (
  stake_address varchar,
  tx_hash text,
  parent_tx_hash text,
  amount text,
  epoch_no word31type,
  block_height word31type,
  block_time integer
)
LANGUAGE sql STABLE
AS $$
  WITH sa_id_list AS (
    SELECT
      id,
      grest.cip5_hex_to_stake_addr(hash_raw)::varchar AS stake_address
    FROM public.stake_address AS sa1
    WHERE sa1.hash_raw = ANY(
        SELECT cardano.bech32_decode_data(n)
        FROM UNNEST(_stake_addresses) AS n
      )
  )
  SELECT
    sa.stake_address,
    ENCODE(tx.hash, 'hex'),
    ENCODE(ptx.hash, 'hex'),
    dd.amount::text,
    b.epoch_no,
    b.block_no,
    EXTRACT(EPOCH FROM b.time)::integer
  FROM sa_id_list AS sa
    INNER JOIN public.direct_deposit AS dd ON dd.addr_id = sa.id
    INNER JOIN public.tx ON tx.id = dd.tx_id
    INNER JOIN public.block AS b ON b.id = tx.block_id
    -- a deposit made in a Dijkstra sub-transaction (CIP-118) also gives the batch's top-level transaction
    LEFT JOIN public.tx AS ptx ON ptx.id = tx.parent_tx_id
  ORDER BY sa.stake_address, b.block_no DESC, dd.id DESC;
$$;

COMMENT ON FUNCTION grest.account_direct_deposits IS 'Get the direct deposits (CIP-159) paid into given stake addresses'; -- noqa: LT01
