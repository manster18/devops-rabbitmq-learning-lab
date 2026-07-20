#!/usr/bin/env bash
# Creates a RabbitMQ virtual host and grants a user full permissions on it,
# using the Management HTTP API. Useful for the per-environment vhost
# isolation lab case (see docs/clustering.md and the Production section).
#
# Usage: ./create-vhost.sh <vhost-name> [user] [password]
set -euo pipefail

readonly MGMT_HOST="${RABBITMQ_MGMT_HOST:-localhost}"
readonly MGMT_PORT="${RABBITMQ_MANAGEMENT_PORT:-15672}"
readonly ADMIN_USER="${RABBITMQ_USER:-guest}"
readonly ADMIN_PASS="${RABBITMQ_PASS:-guest}"

usage() {
  echo "Usage: $0 <vhost-name> [user] [password]"
  echo "  vhost-name  name of the vhost to create (required)"
  echo "  user        user to grant full permissions on the vhost (default: guest)"
  echo "  password    password for 'user', only used if the user does not exist yet"
}

main() {
  if [[ $# -lt 1 ]]; then
    usage
    exit 1
  fi

  local vhost="$1"
  local user="${2:-guest}"
  local password="${3:-guest}"
  local base_url="http://${MGMT_HOST}:${MGMT_PORT}/api"

  echo "Creating vhost '${vhost}'..."
  curl -fsS -u "${ADMIN_USER}:${ADMIN_PASS}" \
    -X PUT "${base_url}/vhosts/${vhost}" \
    -H "content-type: application/json"

  if ! curl -fsS -u "${ADMIN_USER}:${ADMIN_PASS}" "${base_url}/users/${user}" >/dev/null 2>&1; then
    echo "Creating user '${user}'..."
    curl -fsS -u "${ADMIN_USER}:${ADMIN_PASS}" \
      -X PUT "${base_url}/users/${user}" \
      -H "content-type: application/json" \
      -d "{\"password\": \"${password}\", \"tags\": \"\"}"
  fi

  echo "Granting '${user}' full permissions on '${vhost}'..."
  curl -fsS -u "${ADMIN_USER}:${ADMIN_PASS}" \
    -X PUT "${base_url}/permissions/${vhost}/${user}" \
    -H "content-type: application/json" \
    -d '{"configure": ".*", "write": ".*", "read": ".*"}'

  echo "Done. Vhost '${vhost}' is ready for user '${user}'."
}

main "$@"
