"""Kafka producer utilities for publishing water sensor events."""

import json

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SENSOR_TOPIC = "water-sensor-events"


def create_producer():
    """Create and return a configured Kafka producer."""

    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }

    return Producer(config)


def delivery_report(error, message):
    """Report Kafka message delivery failures."""

    if error is not None:
        print(f"Message delivery failed: {error}")


def create_message_key(event):
    """Create a stable Kafka key for a sensor event."""

    return f"{event['sensor_type']}:{event['sensor_id']}"


def serialize_event(event):
    """Serialize a sensor event to JSON."""

    return json.dumps(event)


def publish_sensor_event(producer, event):
    """Publish one sensor event to the Kafka sensor topic."""

    message_key = create_message_key(event)
    message_value = serialize_event(event)

    producer.produce(
        topic=SENSOR_TOPIC,
        key=message_key,
        value=message_value,
        callback=delivery_report,
    )


def flush_producer(producer):
    """Wait for all queued Kafka messages to be delivered."""

    remaining_messages = producer.flush()

    if remaining_messages != 0:
        raise RuntimeError(
            f"{remaining_messages} Kafka messages were not delivered."
        )