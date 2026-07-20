#!/usr/bin/env bash
# Verifies Case 4: topic exchange routing.
# Expected result: 'orders-topic-urgent' and 'orders-topic-regular' both
# received messages (published via `--type mixed`).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 4: topic exchange routing — checking urgent/regular queues..."

urgent_total="$(queue_field orders-topic-urgent messages)"
regular_total="$(mgmt_get "/queues/%2F/orders-topic-regular" | python3 -c "
import json, sys
data = json.load(sys.stdin)
stats = data.get('message_stats', {})
print(stats.get('publish', 0))
")"
urgent_published="$(mgmt_get "/queues/%2F/orders-topic-urgent" | python3 -c "
import json, sys
data = json.load(sys.stdin)
stats = data.get('message_stats', {})
print(stats.get('publish', 0))
")"

check "orders-topic-urgent received at least one message (published total: ${urgent_published})" \
  "$([[ "${urgent_published}" -ge 1 ]] && echo true || echo false)"
check "orders-topic-regular received at least one message (published total: ${regular_total})" \
  "$([[ "${regular_total}" -ge 1 ]] && echo true || echo false)"

echo "  (current depth — urgent: ${urgent_total})"

print_summary
