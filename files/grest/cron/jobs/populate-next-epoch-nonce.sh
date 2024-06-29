#!/bin/bash
DB_NAME=cexplorer
NWMAGIC=
EPOCH_LENGTH=
PROM_URL=
CCLI=
export CARDANO_NODE_SOCKET_PATH=

echo "$(date +%F_%H:%M:%S) Running next epoch nonce calculation..."

# TODO possibly initialize EPOCH_LENGTH from database as well
PROTO_MAJ=`psql ${DB_NAME} -c "select protocol_major from epoch_param where epoch_no = (select max(no) from epoch);" -t`
SECURITY_PARAM=`psql ${DB_NAME} -c "select securityparam from grest.genesis;" -t`
ACTIVE_SLOT_COEFF=`psql ${DB_NAME} -c "select activeslotcoeff from grest.genesis;" -t`
WINDOW_SIZE=2
if [[ $PROTO_MAJ -gt 8 ]]; then
  WINDOW_SIZE=3
fi
#echo "from db: security_param ${SECURITY_PARAM}, active slot coeff ${ACTIVE_SLOT_COEFF}, window size $WINDOW_SIZE based on protocol major $PROTO_MAJ"

min_slot=`echo "${EPOCH_LENGTH} - (${WINDOW_SIZE} * (${SECURITY_PARAM} / ${ACTIVE_SLOT_COEFF}))" | bc`
if [ -z $min_slot ]; then
  min_slot=$((EPOCH_LENGTH * 7 / 10))
  echo "WARNING: Falling back to percent-based calculation, initialized min_slot to ${min_slot}"
fi
#echo "Initialized min_slot to $min_slot"

current_epoch=$(curl -s "${PROM_URL}" | grep epoch | awk '{print $2}')
current_slot_in_epoch=$(curl -s "${PROM_URL}" | grep slotInEpoch | awk '{print $2}')
next_epoch=$((current_epoch + 1))

[[ ${current_slot_in_epoch} -ge ${min_slot} ]] &&
  next_epoch_nonce=$(echo "$(${CCLI} query protocol-state --testnet-magic "${NWMAGIC}" | jq -r .candidateNonce.contents)$(${CCLI} query protocol-state --testnet-magic "${NWMAGIC}" | jq -r .lastEpochBlockNonce.contents)" | xxd -r -p | b2sum -b -l 256 | awk '{print $1}') &&
  psql ${DB_NAME} -c "INSERT INTO grest.epoch_info_cache (epoch_no, p_nonce) VALUES (${next_epoch}, '${next_epoch_nonce}') ON CONFLICT(epoch_no) DO UPDATE SET p_nonce='${next_epoch_nonce}';"

echo "$(date +%F_%H:%M:%S) Job done!"
