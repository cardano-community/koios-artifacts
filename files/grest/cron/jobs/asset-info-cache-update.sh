#!/bin/bash
DB_NAME=cexplorer

echo "$(date +%F_%H:%M:%S) Running asset info cache update..."
psql ${DB_NAME} -qbt -c "SELECT grest.asset_info_cache_update();" 2>&1 1>/dev/null
echo "$(date +%F_%H:%M:%S) Job done!"
