#!/usr/bin/env python3
"""
RabbitMQ Producer — publishes orders with priorities and exchange routing.

Publishes JSON messages with the following fields:
  - order_id: order UUID
  - product: product name
  - quantity: quantity
  - priority: priority (0-10)
  - timestamp: creation time
  - order_type: type (standard, express, bulk)
"""

import json
import os
import sys
import time
import uuid
import argparse
import logging
from datetime import datetime, timezone

import pika
from pika import BasicProperties
from pika.exceptions import UnroutableError, NackError

# ─── Configuration ───────────────────────────────────────────────

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

# Arguments for the 'orders' queue MUST match the definitions in definitions.json,
# otherwise RabbitMQ returns 406 PRECONDITION_FAILED if the queue is declared by
# this service before the broker loads definitions.json.
ORDERS_QUEUE_ARGS = {
    "x-dead-letter-exchange": "orders-dlx",
    "x-dead-letter-routing-key": "orders.dead-letter",
    "x-max-priority": 10,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("producer")


# ─── Sample product catalog ──────────────────────────────────────

PRODUCTS = [
    "Laptop Pro 16",
    "Wireless Mouse MX",
    "Mechanical Keyboard",
    "USB-C Hub 7-in-1",
    "4K Monitor 27\"",
    "Webcam HD",
    "Noise-Canceling Headphones",
    "Portable SSD 1TB",
    "Smart Watch Ultra",
    "Tablet Air",
]

ORDER_TYPES = ["standard", "express", "bulk"]
PRIORITY_MAP = {
    "express": 8,
    "bulk": 3,
    "standard": 5,
}


def generate_order(order_type: str = None, priority_override: int = None) -> dict:
    """
    Generates a sample order.

    Priority is normally derived from order_type (see PRIORITY_MAP), which
    means it is constant for any given type. `priority_override` lets the
    caller force a specific priority regardless of type — this is what makes
    it possible to demonstrate priority-based ordering *within* a single
    queue (see the "Priority Queues" lab case), since only 'standard' orders
    ever reach the 'orders' queue and would otherwise always carry the same
    priority.
    """
    otype = order_type or ORDER_TYPES[uuid.uuid4().int % len(ORDER_TYPES)]
    priority = priority_override if priority_override is not None else PRIORITY_MAP.get(otype, 5)

    return {
        "order_id": str(uuid.uuid4()),
        "product": PRODUCTS[uuid.uuid4().int % len(PRODUCTS)],
        "quantity": (uuid.uuid4().int % 20) + 1,
        "priority": priority,
        "order_type": otype,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_exchange(order: dict) -> str:
    """Determines the target exchange based on order type."""
    otype = order.get("order_type", "standard")

    if otype == "express":
        return "orders-topic"
    elif otype == "bulk":
        return "orders-fanout"
    else:
        return "orders-direct"


def get_routing_key(order: dict, exchange: str) -> str:
    """
    Determines the routing key for `order`, matching the bindings declared
    in definitions.json for the given exchange type. Routing key semantics
    differ per exchange type:
      - direct (orders-direct): must exactly match the binding key
        ("order.new" -> queue "orders").
      - topic (orders-topic): matched against "order.*.urgent" /
        "order.*.regular" patterns based on priority.
      - fanout (orders-fanout): ignored by the broker, kept for logging only.
    """
    if exchange == "orders-direct":
        return "order.new"

    if exchange == "orders-topic":
        otype = order.get("order_type", "standard")
        priority = order.get("priority", 5)
        return f"order.{otype}.urgent" if priority >= 7 else f"order.{otype}.regular"

    return ""  # fanout — routing key is ignored


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

    logger.error("Failed to connect to RabbitMQ after 5 attempts")
    sys.exit(1)


def publish_message(channel, order: dict) -> bool:
    """
    Publishes a message to RabbitMQ.

    Publisher confirms must already be enabled on `channel` (see
    `channel.confirm_delivery()` in `main()`) — enabling it here on every
    call would raise a warning after the first message, since it can only
    be toggled once per channel.

    Args:
        channel: pika channel
        order: order data (dict)

    Returns:
        True if the message was delivered, False otherwise
    """
    exchange = get_exchange(order)
    routing_key = get_routing_key(order, exchange)

    body = json.dumps(order, ensure_ascii=False)
    properties = BasicProperties(
        content_type="application/json",
        delivery_mode=2,  # Persistent
        priority=order.get("priority", 0),
        message_id=order["order_id"],
        timestamp=int(time.time()),
        headers={
            "order_type": order.get("order_type"),
            "source": "learning-lab-producer",
        },
    )

    try:
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body.encode("utf-8"),
            properties=properties,
        )

        logger.info(
            f"Published: order_id={order['order_id'][:8]}... "
            f"product={order['product']} qty={order['quantity']} "
            f"exchange={exchange} routing_key={routing_key}"
        )
        return True

    except UnroutableError:
        logger.error(f"Message unroutable, not delivered: {order['order_id'][:8]}")
        return False
    except NackError:
        logger.error(f"Message nacked by broker: {order['order_id'][:8]}")
        return False


def main():
    parser = argparse.ArgumentParser(description="RabbitMQ Order Producer")
    parser.add_argument(
        "--count", type=int, default=10, help="Number of messages to send"
    )
    parser.add_argument(
        "--delay", type=float, default=0.5, help="Delay between messages (seconds)"
    )
    parser.add_argument(
        "--type",
        choices=["standard", "express", "bulk", "mixed"],
        default="mixed",
        help="Order type (default: mixed)",
    )
    parser.add_argument(
        "--no-confirms", action="store_true", help="Disable publisher confirms"
    )
    parser.add_argument(
        "--priority",
        type=int,
        choices=range(0, 11),
        default=None,
        metavar="0-10",
        help="Override the type-based priority (useful to demonstrate priority ordering)",
    )
    args = parser.parse_args()

    logger.info("Starting Producer...")
    logger.info(f"  RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    logger.info(f"  Messages: {args.count}, delay: {args.delay}s")

    connection = connect()
    channel = connection.channel()

    if not args.no_confirms:
        channel.confirm_delivery()

    # Declare queues in case definitions.json has not been loaded yet.
    # Arguments intentionally match definitions.json (see ORDERS_QUEUE_ARGS),
    # otherwise a mismatched re-declaration raises 406 PRECONDITION_FAILED.
    channel.queue_declare(queue="orders", durable=True, arguments=ORDERS_QUEUE_ARGS)
    channel.queue_declare(queue="orders-dlq", durable=True)

    sent = 0
    failed = 0

    try:
        for i in range(args.count):
            order_type = args.type if args.type != "mixed" else None
            order = generate_order(order_type, priority_override=args.priority)

            success = publish_message(channel, order)

            if success:
                sent += 1
            else:
                failed += 1

            if args.delay > 0 and i < args.count - 1:
                time.sleep(args.delay)

    except KeyboardInterrupt:
        logger.info("Stopped by Ctrl+C")
    finally:
        logger.info(f"Stats: sent={sent}, failed={failed}")
        connection.close()
        logger.info("Connection closed")


if __name__ == "__main__":
    main()
