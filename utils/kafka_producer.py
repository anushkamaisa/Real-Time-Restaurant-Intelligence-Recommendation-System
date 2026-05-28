import argparse
import os
import json
import random
import time
from kafka import KafkaProducer, errors

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "dinevision-events")
KAFKA_ENABLED = os.environ.get("KAFKA_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")
KAFKA_RETRY_INTERVAL_SECONDS = int(os.environ.get("KAFKA_RETRY_INTERVAL_SECONDS", "5"))
KAFKA_RETRY_ATTEMPTS = int(os.environ.get("KAFKA_RETRY_ATTEMPTS", "12"))
KAFKA_RETRY_DELAY_SECONDS = float(os.environ.get("KAFKA_RETRY_DELAY_SECONDS", "5"))

_producer = None
_producer_last_failed_at = 0
_producer_error_logged = False


def get_kafka_producer():
    global _producer, _producer_last_failed_at, _producer_error_logged
    if not KAFKA_ENABLED:
        return None

    now = time.time()
    if _producer is not None:
        return _producer

    if now < _producer_last_failed_at + KAFKA_RETRY_INTERVAL_SECONDS:
        return None

    try:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=10000,
        )
        _producer_error_logged = False
        return _producer
    except errors.NoBrokersAvailable as exc:
        _producer_last_failed_at = now
        if not _producer_error_logged:
            print(f"[KafkaProducer] No brokers available at {KAFKA_BOOTSTRAP_SERVERS}: {exc}")
            _producer_error_logged = True
    except Exception as exc:
        _producer_last_failed_at = now
        if not _producer_error_logged:
            print(f"[KafkaProducer] Failed to create producer: {exc}")
            _producer_error_logged = True

    _producer = None
    return None


def send_event(event_type, data, topic_name=None):
    global _producer, _producer_error_logged
    producer = get_kafka_producer()
    if producer is None:
        return False

    topic = topic_name or DEFAULT_KAFKA_TOPIC
    payload = {
        "event": event_type,
        "timestamp": time.time(),
        **data
    }

    try:
        producer.send(topic, payload, key=event_type.encode("utf-8"))
        producer.flush()
        print(f"Sent to {topic}: {payload}")
        return True
    except errors.NoBrokersAvailable as exc:
        _producer = None
        _producer_error_logged = True
        print(f"[KafkaProducer] No brokers available when sending event: {exc}")
        return False
    except Exception as exc:
        print(f"[KafkaProducer] Failed to send event: {exc}")
        return False


def send_event_with_retry(event_type, data, topic_name=None):
    for attempt in range(1, KAFKA_RETRY_ATTEMPTS + 1):
        print(f"[KafkaProducer] Send attempt {attempt}/{KAFKA_RETRY_ATTEMPTS}...")
        if send_event(event_type, data, topic_name):
            return True
        if attempt < KAFKA_RETRY_ATTEMPTS:
            print(f"[KafkaProducer] Waiting {KAFKA_RETRY_DELAY_SECONDS}s before retrying...")
            time.sleep(KAFKA_RETRY_DELAY_SECONDS)
    print(f"[KafkaProducer] Failed to send event after {KAFKA_RETRY_ATTEMPTS} attempts.")
    return False


def close_producer():
    global _producer
    if _producer is not None:
        try:
            _producer.close()
        except Exception:
            pass
        _producer = None


def generate_sample_event(index):
    cuisines = ["Biryani", "North Indian", "Chinese", "South Indian"]
    return {
        "event": "recommendation",
        "city": random.choice(["Hyderabad", "Delhi", "Mumbai"]),
        "cuisine": random.choice(cuisines),
        "rating": round(random.uniform(3.0, 5.0), 1),
        "votes": random.randint(10, 500),
        "timestamp": time.time(),
        "sample_id": index
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka producer test runner")
    parser.add_argument("--count", type=int, default=10, help="Number of sample events to send")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between events")
    args = parser.parse_args()

    print(f"Kafka producer config: enabled={KAFKA_ENABLED}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}, topic={DEFAULT_KAFKA_TOPIC}")
    if not KAFKA_ENABLED:
        print("Kafka producer is disabled. Set KAFKA_ENABLED=true to enable it.")
        exit(0)

    print(f"Sending {args.count} sample events with {args.interval}s interval...")
    success_count = 0
    for i in range(1, args.count + 1):
        event = generate_sample_event(i)
        result = send_event_with_retry("recommendation", event)
        if result:
            success_count += 1
            print(f"✅ Event #{i} sent: {event}")
        else:
            print(f"❌ Event #{i} failed: {event}")
        time.sleep(args.interval)

    print(f"Finished sending events. Success: {success_count}/{args.count}")
    if success_count == 0:
        print("Kafka is still unavailable. Start Kafka or set KAFKA_BOOTSTRAP_SERVERS to a running broker.")
