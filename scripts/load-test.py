#!/usr/bin/env python3
"""
Load-testing helper for the RabbitMQ Learning Lab.

Publishes a configurable number of messages using multiple concurrent
publisher connections and reports throughput / latency statistics. Useful
for lab case 6 (consumer scaling) and case 7 (memory/disk alarms).

Example:
    python scripts/load-test.py --total 5000 --workers 10 --rate 200
"""

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pika
from pika import BasicProperties

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

ORDERS_QUEUE_ARGS = {
    "x-dead-letter-exchange": "orders-dlx",
    "x-dead-letter-routing-key": "orders.dead-letter",
    "x-max-priority": 10,
}


def build_message(size_bytes: int) -> dict:
    padding = "x" * max(0, size_bytes)
    return {
        "order_id": str(uuid.uuid4()),
        "product": "Load Test Widget",
        "quantity": 1,
        "priority": 5,
        "order_type": "standard",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "padding": padding,
    }


def worker(worker_id: int, count: int, size_bytes: int, transient: bool) -> list:
    """Publishes `count` messages from a dedicated connection and returns latencies (seconds)."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=30,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue="orders", durable=True, arguments=ORDERS_QUEUE_ARGS)
    channel.confirm_delivery()

    # delivery_mode 2 = persistent (default); 1 = transient (stays in RAM longer,
    # useful when demonstrating memory pressure — see lab case 7).
    delivery_mode = 1 if transient else 2

    latencies = []
    try:
        for _ in range(count):
            message = build_message(size_bytes)
            body = json.dumps(message).encode("utf-8")
            properties = BasicProperties(
                content_type="application/json",
                delivery_mode=delivery_mode,
                priority=message["priority"],
            )
            start = time.perf_counter()
            channel.basic_publish(
                exchange="orders-direct",
                routing_key="order.new",
                body=body,
                properties=properties,
            )
            latencies.append(time.perf_counter() - start)
    finally:
        connection.close()

    return latencies


def print_report(latencies: list, elapsed: float, total: int) -> None:
    if not latencies:
        print("No messages were published.")
        return

    sorted_latencies = sorted(latencies)
    p50 = sorted_latencies[len(sorted_latencies) // 2]
    p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
    p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]

    print("─" * 50)
    print("Load test report")
    print("─" * 50)
    print(f"Messages published : {total}")
    print(f"Elapsed time        : {elapsed:.2f}s")
    print(f"Throughput          : {total / elapsed:.1f} msg/s")
    print(f"Latency avg          : {statistics.mean(latencies) * 1000:.2f} ms")
    print(f"Latency p50          : {p50 * 1000:.2f} ms")
    print(f"Latency p95          : {p95 * 1000:.2f} ms")
    print(f"Latency p99          : {p99 * 1000:.2f} ms")


def main() -> int:
    parser = argparse.ArgumentParser(description="RabbitMQ Learning Lab load test")
    parser.add_argument("--total", type=int, default=1000, help="Total messages to publish")
    parser.add_argument("--workers", type=int, default=5, help="Number of concurrent publisher connections")
    parser.add_argument("--size", type=int, default=0, help="Extra padding bytes per message body")
    parser.add_argument(
        "--transient",
        action="store_true",
        help="Publish non-persistent messages (delivery_mode=1); better for memory-pressure demos",
    )
    args = parser.parse_args()

    if args.workers < 1 or args.total < 1:
        print("--total and --workers must be positive integers", file=sys.stderr)
        return 1

    per_worker = args.total // args.workers
    remainder = args.total % args.workers
    counts = [per_worker + (1 if i < remainder else 0) for i in range(args.workers)]

    mode = "transient" if args.transient else "persistent"
    print(
        f"Publishing {args.total} messages using {args.workers} workers "
        f"(size={args.size}, mode={mode})..."
    )
    start = time.perf_counter()
    all_latencies = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(worker, i, count, args.size, args.transient)
            for i, count in enumerate(counts)
            if count > 0
        ]
        for future in as_completed(futures):
            all_latencies.extend(future.result())

    elapsed = time.perf_counter() - start
    print_report(all_latencies, elapsed, args.total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
