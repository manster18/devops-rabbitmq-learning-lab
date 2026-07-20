#!/usr/bin/env bash
# Bootstraps the RabbitMQ Learning Lab stack: starts the core services,
# waits for RabbitMQ to become healthy, and prints the access URLs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly REPO_ROOT
readonly MAX_WAIT_SECONDS=240
readonly POLL_INTERVAL_SECONDS=3

log() {
  printf '[setup] %s\n' "$*"
}

wait_for_rabbitmq() {
  local waited=0
  log "Waiting for RabbitMQ to become healthy (timeout: ${MAX_WAIT_SECONDS}s)..."
  while true; do
    local status
    status="$(docker inspect --format '{{.State.Health.Status}}' rabbitmq 2>/dev/null || echo "starting")"
    if [[ "${status}" == "healthy" ]]; then
      log "RabbitMQ is healthy."
      return 0
    fi
    if (( waited >= MAX_WAIT_SECONDS )); then
      log "Timed out waiting for RabbitMQ to become healthy."
      return 1
    fi
    sleep "${POLL_INTERVAL_SECONDS}"
    waited=$(( waited + POLL_INTERVAL_SECONDS ))
  done
}

ensure_env_file() {
  if [[ ! -f "${REPO_ROOT}/.env" ]]; then
    log "No .env found, creating one from .env.example ..."
    cp "${REPO_ROOT}/.env.example" "${REPO_ROOT}/.env"
  fi
}

main() {
  cd "${REPO_ROOT}"

  ensure_env_file

  log "Starting core services (rabbitmq, prometheus, node-exporter, grafana)..."
  docker compose up -d --build rabbitmq prometheus node-exporter grafana

  wait_for_rabbitmq

  log "Stack is up. Access points:"
  log "  RabbitMQ Management : http://localhost:15672  (guest / guest)"
  log "  Prometheus          : http://localhost:9090"
  log "  Grafana             : http://localhost:3000   (admin / admin)"
  log ""
  log "Run a producer:  docker compose run --rm producer python producer.py --count 10"
  log "Run a consumer:  docker compose run --rm consumer python consumer.py"
}

main "$@"
