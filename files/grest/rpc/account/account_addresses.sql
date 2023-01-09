CREATE OR REPLACE FUNCTION grest.account_addresses (_stake_addresses text[], _first_only boolean default false, _empty boolean default false)
  RETURNS TABLE (
    stake_address varchar,
    addresses json
  )
  LANGUAGE PLPGSQL
  AS $$
DECLARE
  sa_id_list integer[];
BEGIN
  SELECT INTO sa_id_list
    ARRAY_AGG(STAKE_ADDRESS.ID)
  FROM
    STAKE_ADDRESS
  WHERE
    STAKE_ADDRESS.VIEW = ANY(_stake_addresses);

  RETURN QUERY
    WITH txo_addr AS (
      SELECT 
        DISTINCT ON(address) address, 
        stake_address_id 
      FROM
        (
          IF _empty IS NOT TRUE THEN
            SELECT
              txo.address,
              txo.stake_address_id, 
              txo.id
            FROM
              tx_out txo
              LEFT JOIN tx_in ON txo.tx_id = tx_in.tx_out_id
              AND txo.index::smallint = tx_in.tx_out_index::smallint
            WHERE 
              txo.stake_address_id = ANY(sa_id_list)
              AND tx_in.tx_in_id IS NULL
          ELSE
            SELECT
              txo.address, 
              txo.stake_address_id, 
              txo.id
            FROM
              tx_out txo
            WHERE 
              txo.stake_address_id = ANY(sa_id_list)
          END IF
        ) x
      ORDER BY id
      LIMIT 
        CASE WHEN _first_only IS TRUE
          THEN 1
        ELSE
          NULL
        END
    )
    SELECT
      sa.view as stake_address,
      JSON_AGG(txo_addr.address) as addresses
    FROM
      txo_addr
      INNER JOIN STAKE_ADDRESS sa ON sa.id = txo_addr.stake_address_id
    GROUP BY
      sa.id;
END;
$$;

COMMENT ON FUNCTION grest.account_addresses IS 'Get all addresses associated with given accounts, optionally filtered by first used address only or inclusion of used but empty(no utxo) addresses.';

