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
  WITH
    epoch_bounds AS (
      SELECT
        CASE
          WHEN _epoch_no IS NULL THEN NULL
          WHEN (SELECT MAX(epoch_param.epoch_no) FROM public.epoch_param) = _epoch_no::word31type
            THEN (SELECT MAX(id) FROM public.tx)
          ELSE
            (SELECT i_last_tx_id FROM grest.epoch_info_cache WHERE epoch_no = _epoch_no::word31type)
        END AS upper_tx_id,
        CASE
          WHEN _epoch_no IS NULL THEN NULL
          ELSE COALESCE(
            (SELECT i_last_tx_id FROM grest.epoch_info_cache WHERE epoch_no = _epoch_no::word31type - 1),
            (SELECT MAX(i_last_tx_id) FROM grest.epoch_info_cache WHERE epoch_no < _epoch_no::word31type),
            (SELECT MAX(tx.id) FROM public.tx JOIN public.block b ON tx.block_id = b.id WHERE b.epoch_no < _epoch_no::word31type)
          )
        END AS lower_tx_id
    ),
    treasury_reserve_withdrawals AS (
      SELECT
        _epoch_no::word31type AS epoch_no,
        COALESCE((
          SELECT SUM(treasury_donation) FROM public.tx
          WHERE tx.id > (SELECT lower_tx_id FROM epoch_bounds)
            AND tx.id <= (SELECT upper_tx_id FROM epoch_bounds)
        ), 0)::text AS treasury_donation,
        COALESCE((
          SELECT SUM(r.amount) FROM public.reserve r
          WHERE r.tx_id > (SELECT lower_tx_id FROM epoch_bounds)
            AND r.tx_id <= (SELECT upper_tx_id FROM epoch_bounds)
        ), 0)::text AS reserve_withdrawal,
        COALESCE((
          SELECT SUM(t.amount) FROM public.treasury t
          WHERE t.tx_id > (SELECT lower_tx_id FROM epoch_bounds)
            AND t.tx_id <= (SELECT upper_tx_id FROM epoch_bounds)
        ), 0)::text AS treasury_withdrawal
      WHERE _epoch_no IS NOT NULL
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
    LEFT JOIN treasury_reserve_withdrawals trw ON TRUE
  WHERE (_epoch_no IS NOT NULL AND ap.epoch_no = _epoch_no::word31type)
    OR (_epoch_no IS NULL)
  ORDER BY ap.epoch_no DESC;
$$;

COMMENT ON FUNCTION grest.totals IS 'Get the circulating utxo, treasury, rewards, supply and reserves in lovelace for specified epoch, all epochs if empty';
