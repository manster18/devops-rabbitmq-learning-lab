#!/usr/bin/env bash
# Verifies Case 7: memory/disk alarms.
# Expected result: after lowering vm_memory_high_watermark and generating
# load, the node reports an active memory (or disk) alarm.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 7: memory/disk alarms — checking node alarms..."

alarm_count="$(mgmt_get "/nodes" | python3 -c "
import json, sys
nodes = json.load(sys.stdin)
total = sum(len(n.get('alarms', [])) for n in nodes)
print(total)
")"

check "at least one active alarm reported (got: ${alarm_count})" \
  "$([[ "${alarm_count}" -ge 1 ]] && echo true || echo false)"

echo "  Tip: revert vm_memory_high_watermark.absolute back to the relative"
echo "  setting in rabbitmq.conf and restart RabbitMQ once done."

print_summary
