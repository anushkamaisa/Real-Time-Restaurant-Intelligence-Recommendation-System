import sys
import random
import time
from datetime import datetime
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from utils.kafka_producer import send_event

cities = ["Hyderabad", "Delhi", "Mumbai"]
cuisines = ["North Indian", "Chinese", "Biryani", "South Indian"]

def generate_event(event_type="recommendation"):
    return {
        "event": event_type,
        "city": random.choice(cities),
        "cuisine": random.choice(cuisines),
        "rating": round(random.uniform(3.0, 5.0), 1),
        "votes": random.randint(10, 500),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def run_event_loop(interval_seconds=2):
    print("🚀 Event Handler Started... Sending Kafka Events\n")
    try:
        while True:
            event_data = generate_event()
            send_event(event_data["event"], event_data)
            print("✅ Sent:", event_data)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\n🛑 Event Handler Stopped")


if __name__ == "__main__":
    run_event_loop()
