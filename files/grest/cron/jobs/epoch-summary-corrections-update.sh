#!/bin/bash
DB_NAME=cexplorer

tip=$(psql ${DB_NAME} -qbt -c "SELECT EXTRACT(EPOCH FROM time)::integer FROM block ORDER BY id DESC LIMIT 1;" | xargs)

if [[ $(( $(date +%s) - tip )) -gt 300 ]]; then
  echo "$(date +%F_%H:%M:%S) Skipping as database has not received a new block in past 300 seconds!" && exit 1
fi

echo "$(date +%F_%H:%M:%S) Running epoch summary corrections update..."
psql ${DB_NAME} -qbt -c "SELECT GREST.EPOCH_SUMMARY_CORRECTIONS_UPDATE();" 1>/dev/null 2>&1
echo "$(date +%F_%H:%M:%S) Job done!"
