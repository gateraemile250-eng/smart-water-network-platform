"""Consume and validate water sensor events from Apache Kafka."""

import json

from confluent_kafka import Consumer, KafkaError

from src.simulator.sensor_simulator import validate_event


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SENSOR_TOPIC = "water-sensor-events"
CONSUMER_GROUP = "smart-water-validation-consumer"


def create_consumer():
    """Create and return a configured Kafka consumer."""

    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
    }

    return Consumer(config)


def deserialize_event(message_value):
    """Deserialize a Kafka message value from JSON."""

    return json.loads(message_value.decode("utf-8"))


def consume_sensor_events(max_messages=5):
    """Consume and validate a limited number of sensor events."""

    consumer = create_consumer()
    consumer.subscribe([SENSOR_TOPIC])

    consumed_count = 0

    try:
        while consumed_count < max_messages:
            message = consumer.poll(timeout=5.0)

            if message is None:
                print("No message received before timeout.")
                break

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                raise RuntimeError(
                    f"Kafka consumer error: {message.error()}"
                )

            event = deserialize_event(message.value())
            event_valid = validate_event(event)

            message_key = (
                message.key().decode("utf-8")
                if message.key() is not None
                else None
            )

            print(f"\nMessage {consumed_count + 1}")
            print(f"Key: {message_key}")
            print(f"Partition: {message.partition()}")
            print(f"Offset: {message.offset()}")
            print(f"Valid event: {event_valid}")
            print(f"Event: {event}")

            consumed_count += 1

    finally:
        consumer.close()

    print(f"\nTotal messages consumed: {consumed_count}")


def main():
    """Run a short Kafka consumer validation test."""

    consume_sensor_events(max_messages=5)


if __name__ == "__main__":
    main()