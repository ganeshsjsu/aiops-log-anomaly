"""Synthetic log generator that streams events into Kafka."""
from __future__ import annotations

import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from app import settings

fake = Faker()
SERVICES = ["auth-service", "orders-service", "payment-service", "shipping-service"]
LEVELS = ["INFO", "WARN", "ERROR"]
NORMAL_MESSAGES = [
    "Processed request successfully",
    "User session refreshed",
    "Cache hit recorded",
    "Background sync completed",
    "Queued downstream call",
]
ANOMALY_MESSAGES = [
    "Database connection refused",
    "Kafka broker unreachable",
    "Payment gateway HTTP 500",
    "Disk latency above threshold",
    "Null pointer in rule engine",
]


def _build_producer() -> KafkaProducer:
    servers = settings.KAFKA_BOOTSTRAP_SERVERS.split(",")
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=10,
        retries=5,
        acks="all",
    )


def _producer_with_retry(max_attempts: int = 10, base_delay: float = 2.0) -> KafkaProducer:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return _build_producer()
        except NoBrokersAvailable as exc:
            last_error = exc
            wait_time = base_delay * attempt
            print(
                f"Kafka not ready (attempt {attempt}/{max_attempts}). "
                f"Retrying in {wait_time:.1f}s..."
            )
            time.sleep(wait_time)
    raise last_error if last_error else RuntimeError("Kafka bootstrap failed")


def _random_log() -> dict:
    service = random.choice(SERVICES)
    level = random.choices(LEVELS, weights=[0.7, 0.2, 0.1])[0]
    if random.random() < 0.15:
        level = "ERROR"
    is_anomaly = level == "ERROR" and random.random() < 0.7
    message_pool = ANOMALY_MESSAGES if is_anomaly else NORMAL_MESSAGES
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "level": level,
        "host": fake.hostname(),
        "trace_id": uuid4().hex[:8],
        "message": random.choice(message_pool),
        "anomaly_hint": int(is_anomaly),
    }
    return log


def stream_logs() -> None:
    producer = _producer_with_retry()
    print(f"Producing logs to topic {settings.LOG_TOPIC} @ {settings.KAFKA_BOOTSTRAP_SERVERS}")
    while True:
        log_entry = _random_log()
        try:
            producer.send(settings.LOG_TOPIC, value=log_entry)
            producer.flush()
        except Exception as exc:  # re-establish producer if broker connection drops
            print(f"Producer error ({exc}); recreating producer...")
            producer = _producer_with_retry()
            continue
        time.sleep(random.uniform(0.2, 1.2))


if __name__ == "__main__":
    stream_logs()
