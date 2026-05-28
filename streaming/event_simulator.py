import argparse
import sys
import random
import time
from datetime import datetime
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from utils.kafka_producer import send_event_with_retry

cities = ["Hyderabad", "Delhi", "Mumbai"]
cuisines = ["North Indian", "Chinese", "Biryani", "South Indian"]
events = ["recommendation", "rating_prediction", "chat_query"]


def generate_event():
    event_type = random.choice(events)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if event_type == "recommendation":
        return {
            "event": "recommendation",
            "city": random.choice(cities),
            "cuisine": random.choice(cuisines),
            "rating": round(random.uniform(3.0, 5.0), 1),
            "votes": random.randint(10, 500),
            "timestamp": timestamp
        }

    if event_type == "rating_prediction":
        return {
            "event": "rating_prediction",
            "cost": random.randint(200, 2000),
            "votes": random.randint(10, 500),
            "predicted_rating": round(random.uniform(3.0, 5.0), 2),
            "timestamp": timestamp
        }

    return {
        "event": "chat_query",
        "query": random.choice(["best restaurants", "cheap food", "top rated", "near me"]),
        "response": "Sample AI response",
        "timestamp": timestamp
    }


def run_simulator(num_events=0, interval_seconds=2):
    print("🚀 Kafka Event Simulator Started...\n")

    count = 0

    try:
        while num_events == 0 or count < num_events:
            event_data = generate_event()
            event_type = event_data["event"]

            # send to shared topic; event_type is included in the payload
            success = send_event_with_retry(event_type, event_data)
            if success:
                print(f"✅ Sent event #{count + 1} type=[{event_type}]: {event_data}")
            else:
                print(f"❌ Failed to send event #{count + 1} type=[{event_type}]. Continuing to next event.")

            time.sleep(interval_seconds)
            count += 1

    except KeyboardInterrupt:
        print("\n🛑 Simulator Stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Kafka event simulator")
    parser.add_argument("--count", type=int, default=10, help="Number of events to send (0 for continuous)")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds to wait between events")

    args = parser.parse_args()
    print(f"Running simulator for {args.count or 'continuous'} events with interval {args.interval}s")
    run_simulator(num_events=args.count, interval_seconds=args.interval)
