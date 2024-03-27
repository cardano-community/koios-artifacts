#!/bin/bash
DB_NAME=cexplorer

tip=$(psql ${DB_NAME} -qbt -c "select extract(epoch from time)::integer from block order by id desc limit 1;" | xargs)

if [[ $(( $(date +%s) - tip )) -gt 300 ]]; then
  echo "$(date +%F_%H:%M:%S) Skipping as database has not received a new block in past 300 seconds!" && exit 1
fi

asset_registry_exists=$(psql ${DB_NAME} -qbt -c "select last_value from grest.control_table where key='asset_registry_commit';" | xargs)

[[ -z "${asset_registry_exists}" ]] && echo "$(date +%F_%H:%M:%S) Skipping as asset registry cache does not seem to be populated!" && exit 1

echo "$(date +%F_%H:%M:%S) Running asset info cache update..."
psql ${DB_NAME} -qbt -c "SELECT grest.asset_info_cache_update();" 1>/dev/null
echo "$(date +%F_%H:%M:%S) Job done!"
