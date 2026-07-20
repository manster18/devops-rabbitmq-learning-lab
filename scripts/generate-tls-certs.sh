#!/usr/bin/env bash
# Generates a throwaway CA, server, and client certificate for the TLS bonus
# lab case (see README.md, "Bonus 1: TLS for RabbitMQ connections").
# Output is written to ./tls/ at the repository root (git-ignored).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly REPO_ROOT
readonly TLS_DIR="${REPO_ROOT}/tls"
readonly DAYS="${CERT_DAYS:-365}"

main() {
  mkdir -p "${TLS_DIR}"
  cd "${TLS_DIR}"

  echo "Generating CA..."
  openssl genrsa -out ca.key 2048
  openssl req -x509 -new -nodes -key ca.key -sha256 -days "${DAYS}" \
    -out ca.pem -subj "/CN=RabbitMQ Learning Lab CA"

  echo "Generating server certificate..."
  openssl genrsa -out server.key 2048
  openssl req -new -key server.key -out server.csr -subj "/CN=rabbitmq"
  openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -out server.pem -days "${DAYS}" -sha256

  echo "Generating client certificate..."
  openssl genrsa -out client.key 2048
  openssl req -new -key client.key -out client.csr -subj "/CN=client"
  openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -out client.pem -days "${DAYS}" -sha256

  rm -f server.csr client.csr

  echo "Done. Certificates written to ${TLS_DIR}/"
  echo "Next: mount ${TLS_DIR} into the rabbitmq container and add the"
  echo "ssl_options.* settings from README.md 'Bonus 1' to rabbitmq.conf."
}

main "$@"
