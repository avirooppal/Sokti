#!/usr/bin/env python3
"""
Sokti OTT Platform - Integration Test: Streaming Pipeline Engine
Tests:
1. Ingestion of valid playback events into ClickHouse
2. Deduplication by event_id
3. Validation and routing of invalid events to DLQ
4. Session aggregation generation in fact_watch_sessions
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import pytest
import requests
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PLAYBACK_TOPIC = "ott.playback.events.v1"
DLQ_TOPIC = "ott.playback.dlq.v1"

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CH_DB = os.getenv("CLICKHOUSE_DB", "sokti")

def execute_ch(query):
    url = f"http://{CH_HOST}:{CH_PORT}/"
    params = {"query": f"{query} FORMAT JSON", "user": CH_USER, "password": CH_PASS}
    res = requests.get(url, params=params, timeout=5)
    return res.json()

def test_streaming_pipeline_end_to_end():
    from streaming.flink.jobs.playback_streaming_pipeline import StreamingPipeline

    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    test_user_id = str(uuid.uuid4())
    test_session_id = str(uuid.uuid4())
    test_content_id = "cnt_mov_0001"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    event_1_id = str(uuid.uuid4())
    event_1 = {
        "event_id": event_1_id,
        "event_type": "video_play",
        "schema_version": "1.0.0",
        "user_id": test_user_id,
        "session_id": test_session_id,
        "device_id": str(uuid.uuid4()),
        "device_type": "smart_tv",
        "app_version": "3.12.0",
        "content_id": test_content_id,
        "position_seconds": 0.0,
        "playback_seconds": 120.0,
        "event_time": now_str,
        "ingestion_time": now_str
    }

    # 1. Produce valid event
    producer.send(PLAYBACK_TOPIC, key=test_user_id.encode(), value=event_1)

    # 2. Produce duplicate event (same event_id)
    producer.send(PLAYBACK_TOPIC, key=test_user_id.encode(), value=event_1)

    # 3. Produce invalid event (missing content_id)
    invalid_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "video_play",
        "schema_version": "1.0.0",
        "user_id": test_user_id,
        "session_id": test_session_id,
        "device_id": str(uuid.uuid4()),
        "device_type": "smart_tv",
        "app_version": "3.12.0",
        "content_id": None, # Missing required content_id
        "position_seconds": 0.0,
        "playback_seconds": 50.0,
        "event_time": now_str,
        "ingestion_time": now_str
    }
    producer.send(PLAYBACK_TOPIC, key=test_user_id.encode(), value=invalid_event)

    # 4. Produce pause event in the same session
    event_2_id = str(uuid.uuid4())
    event_2 = {
        "event_id": event_2_id,
        "event_type": "video_pause",
        "schema_version": "1.0.0",
        "user_id": test_user_id,
        "session_id": test_session_id,
        "device_id": str(uuid.uuid4()),
        "device_type": "smart_tv",
        "app_version": "3.12.0",
        "content_id": test_content_id,
        "position_seconds": 120.0,
        "playback_seconds": 0.0,
        "event_time": now_str,
        "ingestion_time": now_str
    }
    producer.send(PLAYBACK_TOPIC, key=test_user_id.encode(), value=event_2)
    producer.flush()

    # Initialize pipeline with fresh group reading from earliest
    test_group = f"test-pipeline-{uuid.uuid4().hex[:8]}"
    pipeline = StreamingPipeline(
        consumer_group=test_group,
        max_events=1000, # process all available in topic
        timeout_seconds=6
    )
    pipeline.run()

    # Verification:
    # 1. Deduplication caught duplicate
    assert pipeline.dedup_dropped_count >= 1, "Duplicate event was not detected by deduplication cache!"

    # 2. DLQ caught invalid event
    assert pipeline.dlq_count >= 1, "Invalid event was not routed to DLQ!"

    # 3. Check DLQ Kafka topic
    dlq_consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        group_id=f"test-dlq-verifier-{uuid.uuid4().hex[:6]}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000
    )
    dlq_messages = []
    for msg in dlq_consumer:
        dlq_messages.append(msg.value)
        if len(dlq_messages) >= 1:
            break
    dlq_consumer.close()
    print(f"Verified DLQ message received: {len(dlq_messages)} records")

    # 4. Check ClickHouse raw_playback_events
    time.sleep(1)
    ch_raw = execute_ch(f"SELECT count(*) as cnt FROM {CH_DB}.raw_playback_events WHERE user_id = '{test_user_id}'")
    raw_count = int(ch_raw["data"][0]["cnt"])
    assert raw_count >= 2, f"Expected at least 2 valid events in raw_playback_events, found {raw_count}"

    # 5. Check ClickHouse fact_watch_sessions
    ch_sess = execute_ch(f"SELECT watch_seconds, pause_count FROM {CH_DB}.fact_watch_sessions WHERE user_id = '{test_user_id}' AND session_id = '{test_session_id}'")
    assert len(ch_sess["data"]) > 0, "Expected session aggregate in fact_watch_sessions!"
    sess_row = ch_sess["data"][0]
    assert float(sess_row["watch_seconds"]) >= 120.0, f"Expected >= 120 watch_seconds, got {sess_row['watch_seconds']}"
    assert int(sess_row["pause_count"]) >= 1, f"Expected pause_count >= 1, got {sess_row['pause_count']}"

    print("\nStreaming Pipeline Integration Test PASSED successfully!")

if __name__ == "__main__":
    test_streaming_pipeline_end_to_end()
