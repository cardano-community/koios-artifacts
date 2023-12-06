CREATE OR REPLACE FUNCTION grest.epoch_summary_corrections_update()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  curr_epoch_record record := null;
  latest_epoch bigint = (SELECT MAX(no) FROM epoch);
  last_epoch_checked bigint = coalesce((SELECT last_value::bigint FROM grest.control_table WHERE key = 'last_epoch_summary_data_checked'), -1);
BEGIN
  RAISE NOTICE 'Last validated epoch was %', last_epoch_checked;
  IF last_epoch_checked < 0 THEN
    RAISE NOTICE 'Inserting initial record for key last_epoch_summary_data_checked';
    INSERT INTO grest.control_table values('last_epoch_summary_data_checked', 0, null);
  END IF;
  FOR curr_epoch_record IN (
    SELECT b.epoch_no
    FROM
      (SELECT
        no,
        blk_count AS epoch_blk_count,
        tx_count AS epoch_tx_count
      FROM epoch
      WHERE no > last_epoch_checked - 2) AS e,
      (SELECT
        epoch_no,
        COUNT(block_no) AS block_blk_count,
        SUM(tx_count) AS block_tx_count
      FROM block
      WHERE epoch_no > (last_epoch_checked - 2)
      GROUP BY epoch_no) AS b
      WHERE e.no = b.epoch_no
        AND (e.epoch_blk_count != b.block_blk_count OR e.epoch_tx_count != b.block_tx_count)
      ORDER BY b.epoch_no
  ) LOOP
    RAISE NOTICE 'Need to fix up data for epoch %', curr_epoch_record;
    WITH agg_table AS
      ( SELECT
          MIN(block.epoch_no) AS epoch_no,
          SUM(tx.out_sum) AS out_sum,
          SUM(tx.fee) AS fee_sum,
          MIN(block.time) AS start_time,
          MAX(block.time) AS end_time,
          COUNT(tx.id) AS tx_count,
          COUNT(distinct block.block_no) AS blk_count
        FROM block
        LEFT JOIN tx ON block.id = tx.block_id
        WHERE block.epoch_no = curr_epoch_record.epoch_no
      )

    UPDATE epoch
      SET
        out_sum = COALESCE(agg_table.out_sum, 0),
        fees = COALESCE(agg_table.fee_sum, 0),
        tx_count = agg_table.tx_count,
        blk_count = agg_table.blk_count,
        start_time = agg_table.start_time,
        end_time = agg_table.end_time
      FROM agg_table
      WHERE no = agg_table.epoch_no ;

    RAISE NOTICE 'Epoch row for epoch % corrected', curr_epoch_record;
  END LOOP;
  UPDATE grest.control_table SET last_value = latest_epoch::text WHERE key = 'last_epoch_summary_data_checked';
END;
$$;
