CREATE OR REPLACE FUNCTION grest.pool_delegators_history(_pool_bech32 text, _epoch_no word31type DEFAULT NULL)
RETURNS TABLE (
  stake_address varchar,
  amount text,
  epoch_no word31type
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
  _pool_id bigint;
BEGIN
  SELECT id INTO _pool_id FROM pool_hash WHERE pool_hash.hash_raw = cardano.bech32_decode_data(_pool_bech32);
  RETURN QUERY
    SELECT
      grest.cip5_hex_to_stake_addr(sa.hash_raw)::varchar,
      es.amount::text,
      es.epoch_no
    FROM public.epoch_stake AS es
    INNER JOIN public.stake_address AS sa ON es.addr_id = sa.id
    WHERE es.pool_id = _pool_id
      AND (
        CASE
          WHEN _epoch_no IS NULL THEN TRUE
          ELSE es.epoch_no = _epoch_no
        END
      )
    ORDER BY es.epoch_no DESC;
END;
$$;

COMMENT ON FUNCTION grest.pool_delegators_history IS 'Return information about active delegators (incl. history) for a given pool and epoch number - current epoch if not provided.'; --noqa: LT01
