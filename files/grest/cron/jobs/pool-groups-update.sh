#!/bin/bash
DB_NAME=cexplorer
POOL_GROUP_URL=https://raw.githubusercontent.com/cardano-community/pool_groups/refs/heads/main/spos.json

dttime=$(date +%F_%H:%M:%S)

network=$(psql ${DB_NAME} -qbt -c "SELECT network_name from public.meta;")
[[ "${network}" != "mainnet" ]] && echo "pool_groups endpoint is not applicable for networks other than mainnet!" && exit 0

echo "${dttime} - START - Import pool groups"

curl -sfL "${POOL_GROUP_URL}" -o .poolgroups.json

[[ -f '.poolgroups.csv' ]] && rm -f .poolgroups.csv
jq -er '.[] | [ .pool_id_bech32,.group,.ticker,.adastat_group,.balanceanalytics_group ] | @csv' .poolgroups.json > .poolgroups.csv

cat << EOF > .poolgroups.sql
CREATE TEMP TABLE tmppoolgrps (like grest.pool_groups);
\COPY tmppoolgrps FROM '.poolgroups.csv' DELIMITER ',' CSV;
INSERT INTO grest.pool_groups SELECT * FROM tmppoolgrps ON CONFLICT(pool_id_bech32) DO UPDATE SET pool_id_bech32=excluded.pool_id_bech32, pool_group=excluded.pool_group, ticker=excluded.ticker, adastat_group=excluded.adastat_group, balanceanalytics_group=excluded.balanceanalytics_group;
EOF

psql ${DB_NAME} -qb -f .poolgroups.sql >/dev/null && rm -f .poolgroups.sql
psql ${DB_NAME} -qb -c "INSERT INTO grest.control_table (key, last_value) VALUES ('pool_groups_check','${dttime}') ON CONFLICT(key) DO UPDATE SET last_value='${dttime}'"
echo "$(date +%F_%H:%M:%S) - END - Pool Groups Update finished."