#!/usr/bin/env bash

HAPROXY_SOCKET_PATH="/opt/cardano/cnode/sockets/haproxy.socket"
TESTS_DIR_PATH="/opt/cardano/cnode/koios-artifacts/tests"
VENV_DIR_PATH="${TESTS_DIR_PATH}/koios-tests"
API_SPEC_PATH="/opt/cardano/cnode/koios-artifacts/specs/results/koiosapi-guild.yaml"

source "${VENV_DIR_PATH}/bin/activate"

# Determine a healthy instance to run checks against
TRUSTED_INSTANCES=("http://127.0.0.1:8053" "https://koios-guild.ahlnet.nu:2165" "http://95.216.188.94:8053")
for ((i = 0; i < ${#TRUSTED_INSTANCES[@]} - 1; i++)); do
  echo "Comparing instance: ${TRUSTED_INSTANCES[$i]}"
  for compare_instance in "${TRUSTED_INSTANCES[@]:${i}+1}"; do
    echo "  Comparing against: ${compare_instance}"
    pytest ${TESTS_DIR_PATH} \
      --local-url "${TRUSTED_INSTANCES[$i]}"/api/v0 \
      --compare-url "${compare_instance}"/api/v0 \
      --api-schema-file "${API_SPEC_PATH}" -x -q --tb=line
    result_code=$?

    if [ $result_code -eq 0 ]; then
      HEALTHY_INSTANCE1=${TRUSTED_INSTANCES[$i]}
      HEALTHY_INSTANCE2=${compare_instance}
      break 2
    fi
  done
done

if [ -z "${HEALTHY_INSTANCE1}" ]; then
  echo "Could not deteremine a healthy instance to compare against!"
  exit 1
fi
echo "Healthy instance determined: ${HEALTHY_INSTANCE1}"
echo ""

# Test each server and mark down/bring up from maintenance
readarray -t servers_array < <(echo "show servers state grest_core" | nc -U "$HAPROXY_SOCKET_PATH" | grep 'grest_core' | awk '{print $4 " " $5 " " $18 " " $19 " " $7}')
for server in "${servers_array[@]}"; do
  IFS=" " read name ip dns port state <<<"${server}"
  if [[ $name == *-ssl ]]; then
    test_instance="https://"${dns}:${port}
  else
    test_instance="http://"${ip}:${port}
  fi

  # Don't run checks against healthy instances, make sure they are not in MAINT state
  if [[ "${test_instance}" == "${HEALTHY_INSTANCE1}" || "${test_instance}" == "${HEALTHY_INSTANCE2}" ]]; then
    if [[ "${state}" -ne 0 ]]; then
      echo "Marking server back up from maintenance: ${name}"
      echo "enable server grest_core/${name}" | nc -U "$HAPROXY_SOCKET_PATH"
    fi
    continue
  fi

  echo "Running data tests for: ${name} ${ip} ${dns} ${port}"

  pytest ${TESTS_DIR_PATH} \
    --local-url "${test_instance}"/api/v0 \
    --compare-url "${HEALTHY_INSTANCE1}"/api/v0 \
    --api-schema-file "${API_SPEC_PATH}" -x -q --tb=line
  result_code=$?

  if [[ "$result_code" -ne 0 ]]; then
    if [[ "${state}" -ne 1 ]]; then
      echo "Marking server down for maintenance: ${name}"
      echo "disable server grest_core/${name}" | nc -U "$HAPROXY_SOCKET_PATH"
    else
      echo "Server already marked down for maintenance: ${name}"
    fi
  elif [[ "${state}" -ne 0 ]]; then
    echo "Marking server back up from maintenance: ${name}"
    echo "enable server grest_core/${name}" | nc -U "$HAPROXY_SOCKET_PATH"
  fi
done
