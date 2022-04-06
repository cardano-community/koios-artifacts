#!/usr/bin/env bash

HAPROXY_SOCKET_PATH="/opt/cardano/cnode/sockets/haproxy.socket"
VENV_DIR_PATH="/opt/cardano/cnode/tests/koios-artifacts/tests/koios-tests"
API_SPEC_PATH="/opt/cardano/cnode/tests/koios-artifacts/specs/results/koiosapi-guild.yaml"

source "${VENV_DIR_PATH}/bin/activate"

# This is pseudo-code, need to from haproxy:
# 1) ssl vs no ssl servers
# 2) correct port combination for server
# 3) correct server name (for toggling MAINT state)
# Below line just gets resolved server IP addresses into array
servers=$(echo "show servers state" | nc -U "$HAPROXY_SOCKET_PATH" | grep grest_core | cut -d' ' -f5)
mapfile -t serversArray <<<"${servers}"

for server in "${serversArray[@]}"; do
  echo "Running data tests for: $server"
  pytest /opt/cardano/cnode/tests/koios-artifacts/tests \
    --local-url https://eden-guildnet.koios.rest:8453/api/v0/ \
    --compare-url https://guild.koios.rest/api/v0/ \
    --api-schema-file /opt/cardano/cnode/tests/koios-artifacts/specs/results/koiosapi-guild.yaml -x -v
  resultCode=$?

  echo "Result code: $resultCode"
  if [[ "$resultCode" -ne 0 ]]; then
    echo "Marking server for maintenance: $server"
    # Need to get the correct ${server_name} here from haproxy
    echo "disable server grest_core/${server_name}" | nc -U "$HAPROXY_SOCKET_PATH"
  fi
done
