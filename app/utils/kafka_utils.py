"""Kafka utilities."""

from confluent_kafka import Producer
import json
import logging

logger = logging.getLogger(__name__)


def get_kafka_producer(bootstrap_servers: str) -> Producer:
    """Create and return a Kafka producer."""
    config = {"bootstrap.servers": bootstrap_servers}
    return Producer(config)


def delivery_report(err, msg):
    """Callback for Kafka producer to confirm message delivery."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def push_to_kafka(producer: Producer, topic: str, data: dict, key: str = None):
    """
    Push data to Kafka topic.

    Args:
        producer: Kafka producer instance
        topic: Kafka topic name
        data: Data to push (will be JSON serialized)
        key: Optional message key (for ordering)
    """
    try:
        key_bytes = key.encode("utf-8") if key else None
        producer.produce(
            topic,
            key=key_bytes,
            value=json.dumps(data).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # Trigger delivery callbacks
        logger.info(f"Pushed message to {topic} with key {key}")
    except Exception as e:
        logger.error(f"Error pushing to Kafka: {e}")
