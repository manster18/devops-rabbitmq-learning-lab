#!/usr/bin/env bash
# Verifies Case 3: Dead Letter Queue.
# Expected result: some messages ended up in 'orders-dlq' after repeated failures.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 3: Dead Letter Queue — verifying 'orders-dlq' received messages..."

dlq_messages="$(queue_field orders-dlq messages)"

check "orders-dlq has at least one message (got: ${dlq_messages})" \
  "$([[ "${dlq_messages}" -ge 1 ]] && echo true || echo false)"

print_summary
