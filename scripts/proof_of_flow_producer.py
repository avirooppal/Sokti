#!/usr/bin/env python3
"""
Sokti OTT Platform - Proof of Flow Producer
Emits test playback events to Kafka topic: ott.playback.events.v1
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "ott.playback.events.v1"

def create_producer(retries=10, delay=3):
    print(f"Connecting to Kafka broker at {BOOTSTRAP_SERVERS}...")
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3
            )
            print("Connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka broker not ready yet (attempt {i+1}/{retries}). Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {BOOTSTRAP_SERVERS} after {retries} attempts.")

def generate_sample_events(count=10):
    events = []
    user_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    device_id = str(uuid.uuid4())
    content_id = "cnt_mov_0001"

    event_sequence = [
        ("video_play", 0.0, 0.0),
        ("video_pause", 120.5, 120.5),
        ("video_play", 120.5, 120.5),
        ("video_seek", 450.0, 120.5),
        ("video_complete", 7200.0, 7150.0),
    ]

    now = datetime.now(timezone.utc)
    for i in range(count):
        etype, pos, played = event_sequence[i % len(event_sequence)]
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": etype,
            "schema_version": "1.0.0",
            "user_id": user_id,
            "session_id": session_id,
            "device_id": device_id,
            "device_type": "smart_tv",
            "app_version": "3.12.0",
            "content_id": content_id,
            "position_seconds": float(pos),
            "playback_seconds": float(played),
            "event_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "ingestion_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        }
        events.append(event)
    return events

def produce_events(count=10):
    producer = create_producer()
    events = generate_sample_events(count)

    print(f"Producing {len(events)} sample playback events to topic '{TOPIC}'...")
    for ev in events:
        # Partition by user_id for deterministic in-order per-user stream processing
        key = ev["user_id"]
        future = producer.send(TOPIC, key=key, value=ev)
        record_metadata = future.get(timeout=10)
        print(f" -> Sent {ev['event_type']} [ID: {ev['event_id']}] to partition {record_metadata.partition} offset {record_metadata.offset}")

    producer.flush()
    producer.close()
    print("All test events produced successfully!")

if __name__ == "__main__":
    produce_events(count=10)
