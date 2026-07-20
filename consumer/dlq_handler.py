#!/usr/bin/env python3
"""
Dead Letter Queue handler.

Runs in a background thread inside the consumer process and logs every
message that ends up in the 'orders-dlq' queue. In a real service this is
where you'd persist failed orders, raise alerts, or forward them to an
incident-management system.
"""

import json
import logging
import threading
import time

import pika

logger = logging.getLogger("consumer.dlq")


def run_dlq_handler(
    host: str,
    port: int,
    user: str,
    password: str,
    shutdown_event: threading.Event,
) -> None:
    """Consumes and logs messages from the 'orders-dlq' queue until shutdown."""
    logger.info("DLQ handler started")

    while not shutdown_event.is_set():
        try:
            credentials = pika.PlainCredentials(user, password)
            params = pika.ConnectionParameters(
                host=host,
                port=port,
                credentials=credentials,
            )
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.queue_declare(queue="orders-dlq", durable=True)
            channel.basic_qos(prefetch_count=10)

            def dlq_callback(ch, method, properties, body):
                try:
                    message = json.loads(body.decode("utf-8"))
                    logger.warning(
                        f"DLQ: order_id={message.get('order_id', 'unknown')[:8]}... "
                        f"reason={message.get('_dlq_reason', 'unknown')} "
                        f"at={message.get('_dlq_timestamp', 'N/A')}"
                    )
                except Exception:
                    logger.warning(f"DLQ: failed to parse message — {body[:100]}")

                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue="orders-dlq",
                on_message_callback=dlq_callback,
                auto_ack=False,
            )

            logger.info("DLQ handler: waiting for messages on 'orders-dlq'...")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            logger.warning("DLQ handler: connection lost, reconnecting in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"DLQ handler: error — {e}")
            time.sleep(5)
