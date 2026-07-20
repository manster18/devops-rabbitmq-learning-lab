#!/usr/bin/env bash
# Tears down the lab stack between lab cases. By default it only purges the
# 'orders' and 'orders-dlq' queues over the Management API; pass --full to
# also stop containers and remove all persisted volumes (fresh start).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly REPO_ROOT
readonly MGMT_HOST="${RABBITMQ_MGMT_HOST:-localhost}"
readonly MGMT_PORT="${RABBITMQ_MANAGEMENT_PORT:-15672}"
readonly ADMIN_USER="${RABBITMQ_USER:-guest}"
readonly ADMIN_PASS="${RABBITMQ_PASS:-guest}"

usage() {
  echo "Usage: $0 [--full]"
  echo "  (no args)  purge the 'orders' and 'orders-dlq' queues, keep the stack running"
  echo "  --full     docker compose down -v (stops everything, removes volumes)"
}

purge_queue() {
  local queue="$1"
  local base_url="http://${MGMT_HOST}:${MGMT_PORT}/api"
  echo "Purging queue '${queue}'..."
  curl -fsS -u "${ADMIN_USER}:${ADMIN_PASS}" \
    -X DELETE "${base_url}/queues/%2F/${queue}/contents" \
    || echo "  (skipped — queue not reachable, is RabbitMQ running?)"
}

main() {
  cd "${REPO_ROOT}"

  if [[ "${1:-}" == "--full" ]]; then
    echo "Stopping the stack and removing all volumes..."
    docker compose down -v
    docker compose -f docker-compose.cluster.yml down -v 2>/dev/null || true
    echo "Done. Everything has been reset to a clean state."
    return 0
  fi

  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    return 0
  fi

  purge_queue "orders"
  purge_queue "orders-dlq"
  echo "Done. Queues purged, stack left running."
}

main "$@"
