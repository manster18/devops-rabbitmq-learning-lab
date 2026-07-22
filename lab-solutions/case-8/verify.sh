#!/usr/bin/env bash
# Verifies Case 8: 2-node cluster with quorum queues.
# Run against the cluster stack (docker-compose.cluster.yml), management API
# on rabbitmq-1 (localhost:15672). Both nodes must be up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 8: cluster — checking node count and quorum queue..."

node_count="$(mgmt_get "/nodes" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")"
check "cluster reports 2 nodes (got: ${node_count})" \
  "$([[ "${node_count}" -eq 2 ]] && echo true || echo false)"

queue_json="$(mgmt_get "/queues/%2F/orders-quorum" 2>/dev/null || echo '{}')"

queue_type="$(echo "${queue_json}" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print(data.get('type', ''))
except Exception:
    print('')
")"

members="$(echo "${queue_json}" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    members = data.get('members') or []
    print(len(members))
except Exception:
    print(0)
")"

online="$(echo "${queue_json}" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    online = data.get('online') or []
    print(len(online))
except Exception:
    print(0)
")"

check "orders-quorum queue exists with type 'quorum' (got: '${queue_type}')" \
  "$([[ "${queue_type}" == "quorum" ]] && echo true || echo false)"
check "orders-quorum has 2 members (got: ${members})" \
  "$([[ "${members}" -eq 2 ]] && echo true || echo false)"
check "orders-quorum has 2 online replicas (got: ${online})" \
  "$([[ "${online}" -eq 2 ]] && echo true || echo false)"

print_summary
