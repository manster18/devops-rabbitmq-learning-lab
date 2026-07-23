#!/usr/bin/env bash
# Generates a throwaway CA, server, and client certificate for the TLS bonus
# lab case (see README.md, "Bonus 1: TLS for RabbitMQ connections").
# Output is written to ./tls/ at the repository root (git-ignored).
#
# Server cert includes SANs for DNS:rabbitmq, DNS:localhost, IP:127.0.0.1
# so hostname verification works both from Docker network and from the host.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly REPO_ROOT
readonly TLS_DIR="${REPO_ROOT}/tls"
readonly DAYS="${CERT_DAYS:-365}"

main() {
  command -v openssl >/dev/null 2>&1 || {
    echo "openssl is required but was not found in PATH" >&2
    exit 1
  }

  mkdir -p "${TLS_DIR}"
  cd "${TLS_DIR}"

  echo "Generating CA..."
  openssl genrsa -out ca.key 2048
  openssl req -x509 -new -nodes -key ca.key -sha256 -days "${DAYS}" \
    -out ca.pem -subj "/CN=RabbitMQ Learning Lab CA"

  echo "Generating server certificate (SAN: rabbitmq, localhost, 127.0.0.1)..."
  openssl genrsa -out server.key 2048
  openssl req -new -key server.key -out server.csr -subj "/CN=rabbitmq"
  cat > server.ext <<'EOF'
subjectAltName = DNS:rabbitmq,DNS:localhost,IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF
  openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -out server.pem -days "${DAYS}" -sha256 \
    -extfile server.ext

  echo "Generating client certificate..."
  openssl genrsa -out client.key 2048
  openssl req -new -key client.key -out client.csr -subj "/CN=client"
  cat > client.ext <<'EOF'
extendedKeyUsage = clientAuth
EOF
  openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -out client.pem -days "${DAYS}" -sha256 \
    -extfile client.ext

  # RabbitMQ (non-root) must be able to read key material inside the container.
  chmod 644 ca.pem server.pem server.key client.pem client.key

  rm -f server.csr client.csr server.ext client.ext

  echo
  echo "Done. Certificates written to ${TLS_DIR}/"
  ls -la "${TLS_DIR}"
  echo
  echo "Next steps are in README.md → Bonus 1: TLS for RabbitMQ connections"
}

main "$@"
