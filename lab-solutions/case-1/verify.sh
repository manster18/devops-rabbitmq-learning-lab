#!/usr/bin/env bash
# Verifies Case 1: basic publish/consume flow.
# Expected result: after producer + consumer have run, the 'orders' queue is empty.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 1: publish/consume — verifying 'orders' queue is drained..."

ready="$(queue_field orders messages_ready)"
unacked="$(queue_field orders messages_unacknowledged)"

check "messages_ready is 0 (got: ${ready})" "$([[ "${ready}" == "0" ]] && echo true || echo false)"
check "messages_unacknowledged is 0 (got: ${unacked})" "$([[ "${unacked}" == "0" ]] && echo true || echo false)"

print_summary
