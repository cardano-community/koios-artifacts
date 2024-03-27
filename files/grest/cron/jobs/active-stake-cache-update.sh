#!/bin/bash
DB_NAME=cexplorer

tip=$(psql ${DB_NAME} -qbt -c "select extract(epoch from time)::integer from block order by id desc limit 1;" | xargs)

[[ $(( $(date +%s) - tip )) -gt 300 ]] &&
  echo "$(date +%F_%H:%M:%S) Skipping as database has not received a new block in past 300 seconds!" &&
  exit 1

echo "$(date +%F_%H:%M:%S) Running active stake cache update..."

# High level check in db to see if update needed at all (should be updated only once on epoch transition)
[[ $(psql ${DB_NAME} -qbt -c "SELECT grest.active_stake_cache_update_check();" | tail -2 | tr -cd '[:alnum:]') != 't' ]] &&
  echo "No update needed, exiting..." &&
  exit 0

db_next_epoch_no=$(psql ${DB_NAME} -qbt -c "SELECT MAX(NO)+1 from EPOCH;" | tr -cd '[:alnum:]')
db_epoch_stakes_no=$(psql ${DB_NAME} -qbt -c "SELECT MAX(epoch_no) FROM EPOCH_STAKE;" | tr -cd '[:alnum:]')

[[ ${db_next_epoch_no} -gt ${db_epoch_stakes_no} ]] &&
  echo "Epoch Stake is not populated for epoch ${db_next_epoch_no}, exiting..." &&
  exit 1

psql ${DB_NAME} -qbt -c "SELECT GREST.active_stake_cache_update(${db_epoch_stakes_no});" 1>/dev/null
echo "$(date +%F_%H:%M:%S) Job done!"
