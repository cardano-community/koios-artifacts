CREATE OR REPLACE FUNCTION grest.pool_voting_power_history(_epoch_no numeric DEFAULT NULL, _pool_bech32 text DEFAULT NULL)
RETURNS TABLE (
  pool_id_bech32 text,
  epoch_no word31type,
  amount text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ph.view AS pool_id_bech32,
    ps.epoch_no,
    ps.voting_power::text AS amount
  FROM public.pool_hash AS ph
    INNER JOIN public.pool_stat AS ps ON ph.id = ps.pool_hash_id
  WHERE (CASE WHEN _epoch_no IS NULL THEN TRUE ELSE ps.epoch_no = _epoch_no END)
    AND (CASE
        WHEN _pool_bech32 IS NULL THEN TRUE
        ELSE ph.hash_raw = cardano.bech32_decode_data(_pool_bech32)
      END)
  ORDER BY ph.view, ps.epoch_no DESC;
$$;

COMMENT ON FUNCTION grest.pool_voting_power_history IS 'Get all SPO votes cast for a given pool'; -- noqa: LT01
