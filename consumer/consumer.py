#!/usr/bin/env python3
"""
RabbitMQ Consumer — processes orders with retry and a Dead Letter Queue.

Features:
  - prefetch_count=5 (configurable via ENV)
  - simulates processing with a random delay
  - simulates random failures
  - after MAX_RETRIES failed attempts, sends the message to the DLQ
  - runs the DLQ handler in a background thread
"""

import json
import os
import random
import sys
import time
import signal
import threading
import logging
from datetime import datetime, timezone

import pika
from pika import BasicProperties

from dlq_handler import run_dlq_handler

# ─── Configuration ───────────────────────────────────────────────

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
PROCESSING_MIN_DELAY = float(os.getenv("PROCESSING_MIN_DELAY", "0.5"))
PROCESSING_MAX_DELAY = float(os.getenv("PROCESSING_MAX_DELAY", "2.0"))
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.15"))  # 15% chance of failure

# Must match definitions.json — see producer/producer.py for details.
ORDERS_QUEUE_ARGS = {
    "x-dead-letter-exchange": "orders-dlx",
    "x-dead-letter-routing-key": "orders.dead-letter",
    "x-max-priority": 10,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("consumer")

shutdown_event = threading.Event()


# ─── Connection ──────────────────────────────────────────────────

def connect() -> pika.BlockingConnection:
    """Connects to RabbitMQ with retries."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    for attempt in range(1, 6):
        try:
            connection = pika.BlockingConnection(params)
            logger.info(f"Connected to RabbitMQ (attempt {attempt})")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"Attempt {attempt}/5: failed to connect — {e}")
            time.sleep(5)

    logger.error("Failed to connect to RabbitMQ")
    sys.exit(1)


# ─── Message processing ─────────────────────────────────────────

def process_order(body: bytes) -> bool:
    """
    Processes an order.

    Simulates processing with a random delay and random failures.

    Args:
        body: message body (JSON bytes)

    Returns:
        True if processing succeeded, False on failure
    """
    try:
        order = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Failed to decode message: {e}")
        return False  # Not retryable — the message is invalid

    order_id = order.get("order_id", "unknown")[:8]
    product = order.get("product", "unknown")
    quantity = order.get("quantity", 0)

    logger.info(f"Processing order: {order_id}... product={product} qty={quantity}")

    delay = random.uniform(PROCESSING_MIN_DELAY, PROCESSING_MAX_DELAY)
    time.sleep(delay)

    if random.random() < FAILURE_RATE:
        error_type = random.choice(
            [
                "payment_timeout",
                "insufficient_stock",
                "database_connection_lost",
                "external_api_error",
            ]
        )
        logger.error(f"Failed to process order {order_id}...: {error_type}")
        return False

    logger.info(
        f"Order {order_id}... processed in {delay:.2f}s "
        f"(product={product}, qty={quantity})"
    )
    return True


def send_to_dlq(channel, body: bytes, properties, retry_count: int):
    """
    Publishes a message to the Dead Letter Queue.

    Args:
        channel: pika channel
        body: message body
        properties: message properties
        retry_count: number of previous attempts
    """
    try:
        order = json.loads(body.decode("utf-8"))
    except Exception:
        order = {"raw": body.decode("utf-8", errors="replace")}

    order["_dlq_reason"] = f"failed_after_{retry_count}_retries"
    order["_dlq_timestamp"] = datetime.now(timezone.utc).isoformat()
    order["_dlq_original_message_id"] = properties.message_id

    dlq_body = json.dumps(order, ensure_ascii=False).encode("utf-8")
    dlq_properties = BasicProperties(
        content_type="application/json",
        delivery_mode=2,
        headers={
            "x-death-reason": "max_retries_exceeded",
            "x-retry-count": retry_count,
        },
    )

    channel.basic_publish(
        exchange="orders-dlx",
        routing_key="orders.dead-letter",
        body=dlq_body,
        properties=dlq_properties,
    )

    logger.warning(
        f"Message sent to DLQ: order_id={order.get('order_id', 'unknown')[:8]}... "
        f"(after {retry_count} attempts)"
    )


def republish_for_retry(channel, body: bytes, properties, retry_count: int):
    """
    Re-publishes `body` straight back onto the 'orders' queue (via the
    nameless default exchange, where routing key = queue name) with
    `x-retry-count` set to the new value.

    This is deliberately NOT done with `basic_nack(requeue=True)`: nack
    puts the message back with its ORIGINAL properties untouched, so a
    header-based retry counter would never actually increment and the
    message would loop between consumer and queue forever, never reaching
    MAX_RETRIES / the DLQ.
    """
    new_headers = dict(properties.headers or {})
    new_headers["x-retry-count"] = retry_count

    new_properties = BasicProperties(
        content_type=properties.content_type,
        delivery_mode=2,
        priority=properties.priority,
        message_id=properties.message_id,
        headers=new_headers,
    )

    channel.basic_publish(
        exchange="",
        routing_key="orders",
        body=body,
        properties=new_properties,
    )


def callback(ch, method, properties, body):
    """Callback for handling messages consumed from the queue."""
    retry_count = 0

    if properties.headers and "x-retry-count" in properties.headers:
        retry_count = properties.headers["x-retry-count"]

    success = process_order(body)

    if success:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    retry_count += 1

    if retry_count >= MAX_RETRIES:
        send_to_dlq(ch, body, properties, retry_count)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Message ack'd after being sent to DLQ")
    else:
        logger.info(
            f"Retrying ({retry_count}/{MAX_RETRIES}): "
            f"{properties.message_id[:8] if properties.message_id else 'unknown'}..."
        )
        republish_for_retry(ch, body, properties, retry_count)
        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    logger.info("Starting Consumer...")
    logger.info(f"  RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    logger.info(f"  Prefetch: {PREFETCH_COUNT}")
    logger.info(f"  Max retries: {MAX_RETRIES}")
    logger.info(f"  Failure rate: {FAILURE_RATE * 100}%")

    dlq_thread = threading.Thread(
        target=run_dlq_handler,
        args=(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, shutdown_event),
        daemon=True,
    )
    dlq_thread.start()

    connection = connect()

    def signal_handler(sig, frame):
        logger.info("Shutdown signal received, closing connection...")
        shutdown_event.set()
        if connection.is_open:
            connection.close()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    channel = connection.channel()

    channel.queue_declare(
        queue="orders",
        durable=True,
        arguments=ORDERS_QUEUE_ARGS,
    )

    channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    channel.basic_consume(
        queue="orders",
        on_message_callback=callback,
        auto_ack=False,
    )

    logger.info("Waiting for messages on queue 'orders'... (Ctrl+C to stop)")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Stopped by Ctrl+C")
        channel.stop_consuming()
    finally:
        connection.close()
        logger.info("Connection closed")


if __name__ == "__main__":
    main()
