CREATE OR REPLACE FUNCTION grest.totals(_epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  epoch_no word31type,
  circulation text,
  treasury text,
  reward text,
  supply text,
  reserves text
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ap.epoch_no,
    ap.utxo::text,
    ap.treasury::text,
    ap.rewards::text,
    (ap.treasury + ap.rewards + ap.utxo + ap.deposits + ap.fees)::text AS supply,
    ap.reserves::text
  FROM public.ada_pots AS ap
  WHERE (_epoch_no IS NOT NULL AND ap.epoch_no = _epoch_no)
    OR (_epoch_no IS NULL)
  ORDER BY ap.epoch_no DESC;
$$;

COMMENT ON FUNCTION grest.totals IS 'Get the circulating utxo, treasury, rewards, supply and reserves in lovelace for specified epoch, all epochs if empty'; -- noqa: LT01
