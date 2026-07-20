#!/usr/bin/env bash
# Verifies Case 6: consumer scaling / load balancing.
# Expected result: multiple consumers are attached to 'orders' simultaneously.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 6: consumer scaling — counting consumers on 'orders'..."

consumer_count="$(queue_field orders consumers)"

check "at least 2 consumers attached (got: ${consumer_count})" \
  "$([[ "${consumer_count}" -ge 2 ]] && echo true || echo false)"

print_summary
