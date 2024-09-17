CREATE OR REPLACE FUNCTION grest.is_dangling_delegation(delegation_id bigint)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
  curr_epoch bigint;
  num_retirements bigint;

BEGIN

  SELECT INTO curr_epoch MAX(no) FROM epoch;
  -- revised logic: 
  -- check for any pool retirement record exists for the pool corresponding to given delegation
  -- pool retiring epoch is current or in the past (future scheduled retirements don't count)
  -- pool retiring epoch is after delegation cert submission epoch 
  -- and there does not exist a pool_update transaction for this pool that came after currently analyzed pool retirement tx 
  -- and before last transaction of the epoch preceeding the pool retirement epoch.. pool update submitted after that point in 
  -- time is too late and pool should have been fully retired
  SELECT INTO num_retirements COUNT(*) 
  FROM delegation AS d
    INNER JOIN pool_retire AS pr ON d.id = delegation_id
      AND pr.hash_id = d.pool_hash_id
      AND pr.retiring_epoch <= curr_epoch 
      AND pr.retiring_epoch > (SELECT b.epoch_no FROM block AS b INNER JOIN tx AS t on t.id = d.tx_id and t.block_id = b.id)
      AND NOT EXISTS
        ( SELECT 1
          FROM pool_update AS pu
          WHERE pu.hash_id = d.pool_hash_id
            AND pu.registered_tx_id >= pr.announced_tx_id
            AND pu.registered_tx_id <= (
              SELECT i_last_tx_id 
              FROM grest.epoch_info_cache AS eic
              WHERE eic.epoch_no = pr.retiring_epoch - 1
            )
        );
  
  RETURN num_retirements > 0;
END;
$$;

COMMENT ON FUNCTION grest.is_dangling_delegation IS 'Returns a boolean to indicate whether a given delegation id corresponds to a delegation that has been made dangling by retirement of a stake pool associated with it'; --noqa: LT01
