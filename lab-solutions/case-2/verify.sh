#!/usr/bin/env bash
# Verifies Case 2: prefetch_count effect.
# Checks that a consumer is currently attached to 'orders' with the expected
# prefetch value. Run this WHILE the consumer from the lab step is running.
#
# Usage: ./verify.sh [expected-prefetch]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

expected_prefetch="${1:-}"

echo "Case 2: prefetch_count — inspecting consumers on 'orders'..."

# The Management API exposes consumer info as 'consumer_details' embedded in
# the queue resource itself — there is no separate /queues/.../consumers
# endpoint.
queue_json="$(mgmt_get "/queues/%2F/orders")"
consumer_count="$(echo "${queue_json}" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('consumer_details', [])))")"

check "at least one consumer is attached (got: ${consumer_count})" \
  "$([[ "${consumer_count}" -ge 1 ]] && echo true || echo false)"

if [[ -n "${expected_prefetch}" && "${consumer_count}" -ge 1 ]]; then
  actual_prefetch="$(echo "${queue_json}" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(data['consumer_details'][0].get('prefetch_count', 'unknown'))
")"
  check "prefetch_count matches expected ${expected_prefetch} (got: ${actual_prefetch})" \
    "$([[ "${actual_prefetch}" == "${expected_prefetch}" ]] && echo true || echo false)"
else
  echo "  (pass the expected prefetch as \$1 to also verify the exact value, e.g. './verify.sh 1')"
fi

print_summary
