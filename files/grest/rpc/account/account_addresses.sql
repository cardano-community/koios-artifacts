CREATE OR REPLACE FUNCTION grest.account_addresses(_stake_addresses text [], _first_only boolean DEFAULT FALSE, _empty boolean DEFAULT FALSE)
RETURNS TABLE (
  stake_address varchar,
  addresses jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  sa_id_list integer[];
BEGIN
  SELECT INTO sa_id_list
    ARRAY_AGG(stake_address.ID)
  FROM
    stake_address
  WHERE
    stake_address.hash_raw = ANY(
      SELECT cardano.bech32_decode_data(n)
      FROM UNNEST(_stake_addresses) AS n
    );

  IF _first_only IS NOT TRUE AND _empty IS NOT TRUE THEN
    RETURN QUERY
      WITH txo_addr AS (
        SELECT DISTINCT ON (address)
          address,
          stake_address_id
        FROM
          (
            SELECT
              a.address,
              txo.stake_address_id,
              txo.id
            FROM tx_out AS txo
            INNER JOIN address AS a ON a.id = txo.address_id
            WHERE txo.stake_address_id = ANY(sa_id_list)
              AND txo.consumed_by_tx_id IS NULL
          ) AS x
      )

      SELECT
        grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar AS stake_address,
        JSONB_AGG(txo_addr.address) AS addresses
      FROM
        txo_addr
        INNER JOIN stake_address AS sa ON sa.id = txo_addr.stake_address_id
      GROUP BY
        sa.id;
  ELSE
    RETURN QUERY
      WITH txo_addr AS (
        SELECT DISTINCT ON (address)
          address,
          stake_address_id
        FROM
          (
            SELECT
              a.address,
              txo.stake_address_id,
              txo.id
            FROM tx_out AS txo
            INNER JOIN address AS a ON a.id = txo.address_id
            WHERE txo.stake_address_id = ANY(sa_id_list)
            LIMIT (CASE WHEN _first_only IS TRUE THEN 1 ELSE NULL END)
          ) AS x
      )

      SELECT
        grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar AS stake_address,
        JSONB_AGG(txo_addr.address) AS addresses
      FROM
        txo_addr
        INNER JOIN stake_address AS sa ON sa.id = txo_addr.stake_address_id
      GROUP BY
        sa.id;
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.account_addresses IS 'Get all addresses associated with given accounts, optionally filtered by first used address only or inclusion of used but empty(no utxo) addresses.'; -- noqa: LT01
