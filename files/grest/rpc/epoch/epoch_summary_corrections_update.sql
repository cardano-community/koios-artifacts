
CREATE OR REPLACE function grest.EPOCH_SUMMARY_CORRECTIONS_UPDATE()
RETURNS void
LANGUAGE plpgsql
AS $$

DECLARE
	curr_epoch_record record := null;
	latest_epoch bigint = (select max(no) from epoch);
	last_epoch_checked bigint = coalesce((select last_value::bigint from  grest.control_table where key = 'last_epoch_summary_data_checked'), -1);
BEGIN
	RAISE NOTICE 'Last validated epoch was %', last_epoch_checked;

if last_epoch_checked < 0 then
        RAISE NOTICE 'Inserting initial record for key last_epoch_summary_data_checked';
        insert into grest.control_table values('last_epoch_summary_data_checked', 0, null);
end if;

for curr_epoch_record in (
  select b.epoch_no
  from
    (select no, blk_count as epoch_blk_count, tx_count as epoch_tx_count from epoch) as e,
    (select epoch_no, count (block_no) as block_block_count, sum (tx_count) as block_tx_count
      from block group by epoch_no) as b
    where e.no = b.epoch_no
      and (e.epoch_blk_count != b.block_block_count or e.epoch_tx_count != b.block_tx_count)
      and b.epoch_no > (last_epoch_checked - 2)
    order by b.epoch_no
) loop
      RAISE NOTICE 'Need to fix up data for epoch %', curr_epoch_record;

      with agg_table as
        ( select
            min (block.epoch_no) as epoch_no,
            sum (tx.out_sum) as out_sum,
            sum (tx.fee) as fee_sum,
            min (block.time) as start_time,
            max (block.time) as end_time,
            count (tx.id) as tx_count,
            count (distinct block.block_no) as blk_count
          from
            block left join tx
          on
            block.id = tx.block_id
          where
            block.epoch_no = curr_epoch_record.epoch_no
        )
      update epoch
        set
          out_sum = coalesce(agg_table.out_sum, 0),
          fees = coalesce(agg_table.fee_sum, 0),
          tx_count = agg_table.tx_count,
          blk_count = agg_table.blk_count,
          start_time = agg_table.start_time,
          end_time = agg_table.end_time
        from
          agg_table
        where
          no = agg_table.epoch_no ;

	RAISE NOTICE 'Epoch row for epoch % corrected', curr_epoch_record;
end loop;

update grest.control_table set last_value = latest_epoch::text where key = 'last_epoch_summary_data_checked';

END;
$$;


