#!/usr/bin/env python3
"""
Sokti OTT Platform - Proof of Flow Consumer
Consumes playback events from Kafka (ott.playback.events.v1) and writes them to ClickHouse (sokti.raw_playback_events).
"""

import json
import os
import sys
import time
from datetime import datetime
import requests
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "ott.playback.events.v1"
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "sokti")

def write_to_clickhouse(batch):
    if not batch:
        return
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {
        "query": f"INSERT INTO {CLICKHOUSE_DB}.raw_playback_events FORMAT JSONEachRow",
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD
    }
    payload = "\n".join([json.dumps(row) for row in batch]) + "\n"
    res = requests.post(url, params=params, data=payload, timeout=10)
    if res.status_code != 200:
        raise RuntimeError(f"ClickHouse insert failed (HTTP {res.status_code}): {res.text}")
    print(f" [ClickHouse] Successfully committed {len(batch)} rows to {CLICKHOUSE_DB}.raw_playback_events.")

def run_consumer(max_messages=10, timeout_seconds=15):
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} on topic '{TOPIC}'...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="proof-of-flow-clickhouse-loader",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000
    )

    print(f"Consumer started. Waiting for up to {max_messages} messages (timeout: {timeout_seconds}s)...")
    messages = []
    start_time = time.time()

    while len(messages) < max_messages:
        if time.time() - start_time > timeout_seconds and len(messages) > 0:
            print(f"Timeout reached. Flushing {len(messages)} collected messages.")
            break
        for message in consumer:
            val = message.value
            print(f" -> Consumed {val.get('event_type')} [ID: {val.get('event_id')}] from partition {message.partition} offset {message.offset}")
            messages.append(val)
            if len(messages) >= max_messages:
                break

    if messages:
        write_to_clickhouse(messages)
    else:
        print("No messages consumed.")

    consumer.close()
    print("Consumer finished.")

if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    run_consumer(max_messages=count)
