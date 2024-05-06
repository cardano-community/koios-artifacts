#!/bin/bash
DB_NAME=cexplorer

tip=$(psql ${DB_NAME} -qbt -c "select extract(epoch from time)::integer from block order by id desc limit 1;" | xargs)

[[ $(( $(date +%s) - tip )) -gt 300 ]] &&
  echo "$(date +%F_%H:%M:%S) Skipping as database has not received a new block in past 300 seconds!" &&
  exit 1

echo "$(date +%F_%H:%M:%S) Running active stake cache update..."

# High level check in db to see if update needed at all (should be updated only once for next epoch once epoch stake for it is available)
[[ $(psql ${DB_NAME} -qbt -c "SELECT grest.active_stake_cache_update_check();" | tail -2 | tr -cd '[:alnum:]') != 't' ]] &&
  echo "No update needed, exiting..." &&
  exit 0

next_epoch_no=$(psql ${DB_NAME} -qbt -c "SELECT MAX(epoch_no) FROM epoch_stake_progress WHERE completed='t'" | tr -cd '[:alnum:]')

psql ${DB_NAME} -qbt -c "SELECT GREST.active_stake_cache_update(${next_epoch_no});" 1>/dev/null
echo "$(date +%F_%H:%M:%S) Job done!"
