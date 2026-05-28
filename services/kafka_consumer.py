import os
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_ENABLED = os.environ.get("KAFKA_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")


def create_consumer():
    if not KAFKA_ENABLED:
        return None

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "dinevision-events")

    try:
        return KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
    except KafkaError as exc:
        raise RuntimeError(
            f"Unable to connect to Kafka broker(s) at '{bootstrap_servers}'. "
            "Please ensure Kafka is running and KAFKA_BOOTSTRAP_SERVERS is configured correctly. "
            f"Original error: {exc}"
        ) from exc


if __name__ == "__main__":
    try:
        consumer = create_consumer()
        if consumer is None:
            print("⚠️ Kafka is disabled via KAFKA_ENABLED=false or no broker configured.")
            print("Start Kafka and set KAFKA_ENABLED=true if you want to consume events.")
            print("Example: $env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092'")
            exit(0)

        print("🚀 Kafka Consumer Started")

        for message in consumer:
            print("Received:", message.value)
        print("⏹️ Kafka consumer loop ended")
    except RuntimeError as exc:
        print(f"❌ Kafka Consumer failed: {exc}")
