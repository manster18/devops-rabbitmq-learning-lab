#!/usr/bin/env bash
# Verifies Case 3: Dead Letter Queue.
# Expected result: some messages were published to 'orders-dlq' after repeated
# failures. Depth may be 0 because the built-in DLQ handler (background thread
# in consumer.py) consumes and acks those messages immediately — so we check
# cumulative message_stats, not the current queue depth.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 3: Dead Letter Queue — verifying 'orders-dlq' received messages..."

stats="$(mgmt_get "/queues/%2F/orders-dlq" | python3 -c "
import json, sys
data = json.load(sys.stdin)
ms = data.get('message_stats') or {}
print(ms.get('publish', 0), ms.get('ack', 0), data.get('messages', 0))
")"
read -r published acked depth <<< "${stats}"

check "orders-dlq received at least one message (publish=${published}, depth=${depth})" \
  "$([[ "${published}" -ge 1 ]] && echo true || echo false)"
check "DLQ handler acked at least one message (ack=${acked})" \
  "$([[ "${acked}" -ge 1 ]] && echo true || echo false)"

print_summary
