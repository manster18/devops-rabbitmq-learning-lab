#!/usr/bin/env bash
# Verifies Case 5: priority queues.
# Checks that the 'orders' queue is configured with x-max-priority=10.
# The actual processing order (express before standard) must be observed
# manually via the Management UI "Get messages" panel, as described in the
# README lab steps.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 5: priority queues — checking 'orders' queue arguments..."

max_priority="$(mgmt_get "/queues/%2F/orders" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(data.get('arguments', {}).get('x-max-priority', ''))
")"

check "x-max-priority is 10 (got: ${max_priority})" \
  "$([[ "${max_priority}" == "10" ]] && echo true || echo false)"

print_summary
