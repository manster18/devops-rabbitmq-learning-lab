#!/usr/bin/env bash
# Verifies Case 7: memory/disk alarms.
# Expected result: an active resource alarm on the node.
#
# Note: on RabbitMQ 4.x, /api/nodes mem_alarm can stay false even when an
# alarm is in effect. The reliable signal is /api/health/checks/alarms
# (or Prometheus rabbitmq_alarms_memory_used_watermark).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

echo "Case 7: memory/disk alarms — checking node alarms..."

# health check returns HTTP 200 when clear, 503 when alarms are active.
# curl -f would treat 503 as failure, so we capture code explicitly.
http_code="$(curl -s -o /tmp/rmq-alarms.json -w '%{http_code}' \
  -u "${ADMIN_USER}:${ADMIN_PASS}" \
  "${BASE_URL}/health/checks/alarms")"

alarm_summary="$(python3 -c "
import json
try:
    data = json.load(open('/tmp/rmq-alarms.json'))
except Exception:
    print('unreadable')
    raise SystemExit
alarms = data.get('alarms') or []
if alarms:
    print(','.join(f\"{a.get('node','?')}:{a.get('resource','?')}\" for a in alarms))
else:
    print(data.get('status', 'ok'))
")"

check "active alarm via /api/health/checks/alarms (http=${http_code}, ${alarm_summary})" \
  "$([[ "${http_code}" == "503" ]] && echo true || echo false)"

echo "  Tip: clear the lab alarm with:"
echo "    docker compose exec rabbitmq rabbitmqctl set_vm_memory_high_watermark 0.4"

print_summary
