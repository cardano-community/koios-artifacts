#!/bin/bash
DB_NAME=cexplorer
NWMAGIC=
PROM_URL=
CCLI=
export CARDANO_NODE_SOCKET_PATH=

echo "$(date +%F_%H:%M:%S) - START - CLI Protocol Parameters Update"

last_epoch="$(psql ${DB_NAME} -c "select last_value from grest.control_table where key='cli_protocol_params'" -t | xargs)"
current_epoch=$(curl -s "${PROM_URL}" | grep epoch | awk '{print $2}')

if [[ -z ${current_epoch} ]] || ! [[ ${current_epoch} =~ ^[0-9]+$ ]]; then
  echo "$(date +%F_%H:%M:%S) - Unable to fetch epoch metric from node"
  echo "$(date +%F_%H:%M:%S) - Error message: ${current_epoch}"
  exit 1
fi

[[ -n ${last_epoch} && ${last_epoch} -eq ${current_epoch} ]] && echo "$(date +%F_%H:%M:%S) - END - CLI Protocol Parameters Update, no update necessary." && exit 0

prot_params="$(${CCLI} query protocol-parameters --testnet-magic "${NWMAGIC}" 2>&1)"

if grep -q "Network.Socket.connect" <<< "${prot_params}"; then
  echo "$(date +%F_%H:%M:%S) - Node socket path wrongly configured or node not running, please verify that socket set in env file match what is used to run the node"
  echo "$(date +%F_%H:%M:%S) - Error message: ${prot_params}"
  exit 1
elif [[ -z "${prot_params}" ]] || ! jq -er . <<< "${prot_params}" &>/dev/null; then
  echo "$(date +%F_%H:%M:%S) - Failed to query protocol parameters, ensure your node is running with correct genesis (the node needs to be in sync to 1 epoch after the hardfork)"
  echo "$(date +%F_%H:%M:%S) - Error message: ${prot_params}"
  exit 1
fi

psql ${DB_NAME} -qb -c "INSERT INTO grest.control_table (key, last_value, artifacts) VALUES ('cli_protocol_params','${current_epoch}','${prot_params}') ON CONFLICT(key) DO UPDATE SET last_value='${current_epoch}', artifacts='${prot_params}'"

echo "$(date +%F_%H:%M:%S) - END - CLI Protocol Parameters Update, updated for epoch ${current_epoch}."
