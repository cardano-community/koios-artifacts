CREATE OR REPLACE FUNCTION grest.totals(_epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  epoch_no word31type,
  circulation text,
  treasury text,
  reward text,
  supply text,
  reserves text,
  fees text,
  deposits_stake text,
  deposits_drep text,
  deposits_proposal text,
  treasury_donation text,
  treasury_withdrawal text,
  reserves_withdrawal text
)
LANGUAGE sql STABLE
AS $$
  WITH treasury_reserve_withdrawals AS (
      SELECT
        b.epoch_no,
        COALESCE(SUM(tx.treasury_donation), 0)::text AS treasury_donation,
        COALESCE(SUM(r.amount), 0)::text AS reserve_withdrawal,
        COALESCE(SUM(t.amount), 0)::text AS treasury_withdrawal
      FROM public.tx AS tx
        INNER JOIN public.block AS b ON b.id = tx.block_id
        LEFT JOIN public.reserve AS r ON r.tx_id = tx.id
        LEFT JOIN public.treasury AS t ON t.tx_id = tx.id
      WHERE b.epoch_no = _epoch_no::word31type
      GROUP BY b.epoch_no
  )
  SELECT
    ap.epoch_no,
    ap.utxo::text,
    ap.treasury::text,
    ap.rewards::text,
    (ap.treasury + ap.rewards + ap.utxo + ap.deposits_stake + ap.deposits_drep + ap.deposits_proposal + ap.fees)::text AS supply,
    ap.reserves::text,
    ap.fees::text,
    ap.deposits_stake::text,
    ap.deposits_drep::text,
    ap.deposits_proposal::text,
    trw.treasury_donation::text,
    trw.treasury_withdrawal::text,
    trw.reserve_withdrawal::text
  FROM public.ada_pots AS ap
    INNER JOIN treasury_reserve_withdrawals trw ON TRUE
  WHERE (_epoch_no IS NOT NULL AND ap.epoch_no = _epoch_no)
    OR (_epoch_no IS NULL)
  ORDER BY ap.epoch_no DESC;
$$;

COMMENT ON FUNCTION grest.totals IS 'Get the circulating utxo, treasury, rewards, supply and reserves in lovelace for specified epoch, all epochs if empty';
