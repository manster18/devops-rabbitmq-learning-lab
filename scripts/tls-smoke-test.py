#!/usr/bin/env python3
"""
TLS smoke test for Bonus 1 (mutual TLS to RabbitMQ on port 5671).

Usage (from repo root, with pika installed):
    python scripts/tls-smoke-test.py
    python scripts/tls-smoke-test.py --negative   # also prove plain/no-cert fails

Or via the producer image (has pika):
    docker compose run --rm \\
      -v "$(pwd)/scripts:/app/scripts:ro" \\
      -v "$(pwd)/tls:/certs:ro" \\
      -e RABBITMQ_HOST=rabbitmq \\
      -e TLS_CA=/certs/ca.pem \\
      -e TLS_CERT=/certs/client.pem \\
      -e TLS_KEY=/certs/client.key \\
      producer python scripts/tls-smoke-test.py
"""

from __future__ import annotations

import argparse
import os
import ssl
import sys

import pika
from pika import SSLOptions
from pika.exceptions import AMQPConnectionError, ProbableAuthenticationError


def build_ssl_context(ca: str, cert: str | None, key: str | None) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(ca)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    if cert and key:
        context.load_cert_chain(cert, key)
    return context


def try_connect(
    host: str,
    port: int,
    user: str,
    password: str,
    ssl_context: ssl.SSLContext | None,
    label: str,
) -> bool:
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=pika.PlainCredentials(user, password),
        ssl_options=SSLOptions(ssl_context, server_hostname=host) if ssl_context else None,
        heartbeat=30,
        blocked_connection_timeout=10,
        connection_attempts=1,
        retry_delay=0,
        socket_timeout=10,
    )
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue="tls-smoke-test", durable=False, auto_delete=True)
        channel.basic_publish(
            exchange="",
            routing_key="tls-smoke-test",
            body=b'{"ok":true,"via":"tls-smoke-test"}',
        )
        method, _props, body = channel.basic_get(queue="tls-smoke-test", auto_ack=True)
        connection.close()
        if method is None:
            print(f"[FAIL] {label}: connected but could not round-trip a message")
            return False
        print(f"[PASS] {label}: connected, published and consumed {body!r}")
        return True
    except Exception as exc:  # noqa: BLE001 — smoke test wants any failure visible
        print(f"[FAIL] {label}: {type(exc).__name__}: {exc}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="RabbitMQ mTLS smoke test")
    parser.add_argument("--negative", action="store_true", help="Also verify insecure paths fail")
    args = parser.parse_args()

    host = os.getenv("RABBITMQ_HOST", "localhost")
    tls_port = int(os.getenv("RABBITMQ_TLS_PORT", "5671"))
    plain_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASS", "guest")

    ca = os.getenv("TLS_CA", "tls/ca.pem")
    cert = os.getenv("TLS_CERT", "tls/client.pem")
    key = os.getenv("TLS_KEY", "tls/client.key")

    for path in (ca, cert, key):
        if not os.path.isfile(path):
            print(f"Missing certificate file: {path}", file=sys.stderr)
            print("Run ./scripts/generate-tls-certs.sh first.", file=sys.stderr)
            return 1

    print(f"Target host={host} tls_port={tls_port}")
    ctx = build_ssl_context(ca, cert, key)
    ok = try_connect(host, tls_port, user, password, ctx, "mTLS on 5671")

    if args.negative:
        print("--- negative checks (expected FAIL) ---")
        # TLS port but no client certificate (broker requires peer cert)
        ctx_no_client = build_ssl_context(ca, None, None)
        no_cert_ok = try_connect(
            host, tls_port, user, password, ctx_no_client, "TLS without client cert"
        )
        # Plain AMQP still open in this lab (we do not set listeners.tcp = none)
        plain_ok = try_connect(
            host, plain_port, user, password, None, "plain AMQP on 5672 (still enabled)"
        )
        if no_cert_ok:
            print("[FAIL] broker accepted a connection without client cert — check ssl_options")
            ok = False
        else:
            print("[PASS] broker rejected connection without client cert (as expected)")
        if plain_ok:
            print("[INFO] plain 5672 still works — expected unless you set listeners.tcp = none")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
