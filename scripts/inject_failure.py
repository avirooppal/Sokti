"""
Sokti Failure Injection Script
Simulates production failure scenarios:
1. duplicate: resends identical event_id records to test deduplication
2. schema-drift: injects payload with corrupted/unexpected fields
3. null-content: sends playback event with missing/null content_id
4. late-events: emits events with timestamps far behind current watermark (>7 days old)
5. poison-pill: emits unparseable non-JSON / corrupted payload to test DLQ isolation
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.inject_failure")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PLAYBACK_TOPIC = "ott.playback.events.v1"


def get_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, (dict, list)) else v,
        acks="all",
        retries=3,
    )


def inject_duplicate(producer, count: int = 5):
    """Resend identical event_id multiple times."""
    event_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).isoformat()

    event = {
        "event_id": event_id,
        "event_type": "video_play",
        "schema_version": "1.0.0",
        "user_id": user_id,
        "session_id": session_id,
        "device_id": str(uuid.uuid4()),
        "device_type": "smart_tv",
        "app_version": "3.4.1",
        "content_id": "mov_001",
        "position_seconds": 0.0,
        "playback_seconds": 0.0,
        "event_time": now_str,
        "ingestion_time": now_str,
    }

    logger.info("Injecting %d DUPLICATE events for event_id=%s", count, event_id)
    for i in range(count):
        producer.send(PLAYBACK_TOPIC, key=user_id.encode("utf-8"), value=event)
    producer.flush()
    logger.info("Successfully injected duplicate events.")


def inject_schema_drift(producer):
    """Emit record with invalid schema version and unauthorized fields."""
    user_id = str(uuid.uuid4())
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "video_play",
        "schema_version": "99.0.0-unauthorized",
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "device_id": str(uuid.uuid4()),
        "device_type": "hacked_device",
        "app_version": "unknown",
        "content_id": "mov_001",
        "unexpected_field_abc": "malicious_injection",
        "position_seconds": 12.0,
        "playback_seconds": 12.0,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
    }
    logger.info("Injecting SCHEMA DRIFT event with unauthorized schema_version and extra fields")
    producer.send(PLAYBACK_TOPIC, key=user_id.encode("utf-8"), value=event)
    producer.flush()
    logger.info("Successfully injected schema-drift event.")


def inject_null_content(producer):
    """Emit playback event with empty content_id."""
    user_id = str(uuid.uuid4())
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "video_play",
        "schema_version": "1.0.0",
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "device_id": str(uuid.uuid4()),
        "device_type": "mobile_android",
        "app_version": "3.4.1",
        "content_id": "",  # Corrupted null/empty content_id
        "position_seconds": 0.0,
        "playback_seconds": 0.0,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
    }
    logger.info("Injecting NULL CONTENT event (content_id='')")
    producer.send(PLAYBACK_TOPIC, key=user_id.encode("utf-8"), value=event)
    producer.flush()
    logger.info("Successfully injected null-content event.")


def inject_late_events(producer):
    """Emit event with event_time 10 days in the past."""
    user_id = str(uuid.uuid4())
    old_time = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    now_str = datetime.now(timezone.utc).isoformat()

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "video_play",
        "schema_version": "1.0.0",
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "device_id": str(uuid.uuid4()),
        "device_type": "web_desktop",
        "app_version": "3.4.1",
        "content_id": "mov_002",
        "position_seconds": 120.0,
        "playback_seconds": 120.0,
        "event_time": old_time,
        "ingestion_time": now_str,
    }
    logger.info("Injecting LATE-ARRIVING event with event_time=%s (10 days old)", old_time)
    producer.send(PLAYBACK_TOPIC, key=user_id.encode("utf-8"), value=event)
    producer.flush()
    logger.info("Successfully injected late-arriving event.")


def inject_poison_pill(producer):
    """Inject raw byte string that fails JSON/Avro deserialization."""
    raw_corrupted_payload = b"CORRUPTED_NON_JSON_DATA_BINARY_JUNK_x\x00\x01\x02\xff"
    logger.info("Injecting POISON PILL raw corrupted binary payload into Kafka")
    producer.send(PLAYBACK_TOPIC, key=b"poison_key", value=raw_corrupted_payload)
    producer.flush()
    logger.info("Successfully injected poison-pill record.")


def main():
    parser = argparse.ArgumentParser(description="Sokti Production Failure Injection Utility")
    parser.add_argument(
        "--type",
        required=True,
        choices=["duplicate", "schema-drift", "null-content", "late-events", "poison-pill"],
        help="Type of production failure to simulate",
    )
    parser.add_argument("--count", type=int, default=3, help="Number of times to inject")
    args = parser.parse_args()

    producer = get_producer()
    logger.info("Initialized Kafka producer targeting %s", KAFKA_BOOTSTRAP)

    if args.type == "duplicate":
        inject_duplicate(producer, count=args.count)
    elif args.type == "schema-drift":
        inject_schema_drift(producer)
    elif args.type == "null-content":
        inject_null_content(producer)
    elif args.type == "late-events":
        inject_late_events(producer)
    elif args.type == "poison-pill":
        inject_poison_pill(producer)

    producer.close()
    logger.info("Failure injection completed for type: %s", args.type)


if __name__ == "__main__":
    main()
