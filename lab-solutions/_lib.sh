#!/usr/bin/env bash
# Shared helpers for lab-solutions/case-*/verify.sh scripts.
# Source this file, do not execute it directly.

MGMT_HOST="${RABBITMQ_MGMT_HOST:-localhost}"
MGMT_PORT="${RABBITMQ_MANAGEMENT_PORT:-15672}"
ADMIN_USER="${RABBITMQ_USER:-guest}"
ADMIN_PASS="${RABBITMQ_PASS:-guest}"
BASE_URL="http://${MGMT_HOST}:${MGMT_PORT}/api"

PASS_COUNT=0
FAIL_COUNT=0

mgmt_get() {
  local path="$1"
  curl -fsS -u "${ADMIN_USER}:${ADMIN_PASS}" "${BASE_URL}${path}"
}

# queue_field <queue-name> <json-field>
queue_field() {
  local queue="$1"
  local field="$2"
  mgmt_get "/queues/%2F/${queue}" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(data.get('${field}', ''))
"
}

check() {
  local description="$1"
  local condition="$2"
  if [[ "${condition}" == "true" ]]; then
    echo "  [PASS] ${description}"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "  [FAIL] ${description}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

print_summary() {
  echo "---"
  echo "Passed: ${PASS_COUNT}, Failed: ${FAIL_COUNT}"
  if [[ "${FAIL_COUNT}" -gt 0 ]]; then
    return 1
  fi
  return 0
}
