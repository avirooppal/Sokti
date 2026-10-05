#!/usr/bin/env python3
"""
Sokti OTT Platform - Data Lake Parquet Writer
Consumes raw streaming events from Kafka and writes partitioned Parquet files directly to MinIO (S3-compatible Lake).
Partition paths:
  - raw/playback/date=YYYY-MM-DD/hour=HH/
  - raw/search/date=YYYY-MM-DD/
  - raw/subscriptions/date=YYYY-MM-DD/
"""

import io
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
RAW_BUCKET = "sokti-raw"

TOPICS = [
    "ott.playback.events.v1",
    "ott.search.events.v1",
    "ott.subscription.cdc.v1"
]

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1"
    )

class ParquetLakeWriter:
    def __init__(self, consumer_group="sokti-lake-writer-group", max_messages=None, timeout_seconds=None):
        self.max_messages = max_messages
        self.timeout_seconds = timeout_seconds
        self.s3_client = get_s3_client()
        
        # Buffers partitioned by (category, date_str, hour_str): [records]
        self.buffers = {}
        self.total_written = 0

        self.consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            group_id=consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000
        )

    def _parse_timestamp(self, t_str):
        t_clean = t_str.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(t_clean)
        except Exception:
            return datetime.strptime(t_str[:19], "%Y-%m-%d %H:%M:%S")

    def _flush_buffer(self, buffer_key):
        records = self.buffers.get(buffer_key, [])
        if not records:
            return

        category, date_str, hour_str = buffer_key
        table = pa.Table.from_pylist(records)

        # In-memory buffer for Parquet conversion
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="snappy")
        sink.seek(0)

        # Build S3 Path
        file_uuid = uuid.uuid4().hex[:8]
        if hour_str:
            s3_key = f"raw/{category}/date={date_str}/hour={hour_str}/part_{file_uuid}.parquet"
        else:
            s3_key = f"raw/{category}/date={date_str}/part_{file_uuid}.parquet"

        self.s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=sink.getvalue()
        )

        print(f" [DataLake] Wrote {len(records)} records -> s3://{RAW_BUCKET}/{s3_key}")
        self.total_written += len(records)
        self.buffers[buffer_key] = []

    def flush_all(self):
        for k in list(self.buffers.keys()):
            self._flush_buffer(k)

    def run(self):
        print(f"Starting Parquet Lake Writer on topics {TOPICS} -> MinIO ({MINIO_ENDPOINT}/{RAW_BUCKET})...")
        start_time = time.time()
        last_flush = time.time()
        msg_count = 0

        try:
            while True:
                msg_dict = self.consumer.poll(timeout_ms=1000)
                now = time.time()

                if not msg_dict:
                    if self.timeout_seconds and (now - start_time) >= self.timeout_seconds:
                        print(f"Timeout reached ({self.timeout_seconds}s). Exiting lake writer.")
                        break
                    continue

                for tp, messages in msg_dict.items():
                    topic = tp.topic
                    category = "playback" if "playback" in topic else ("search" if "search" in topic else "subscriptions")

                    for msg in messages:
                        val = msg.value
                        msg_count += 1
                        
                        # Extract event time
                        etime_str = val.get("event_time") or val.get("ingestion_time") or datetime.now(timezone.utc).isoformat()
                        try:
                            dt = self._parse_timestamp(etime_str)
                            date_str = dt.strftime("%Y-%m-%d")
                            hour_str = dt.strftime("%H") if category == "playback" else None
                        except Exception:
                            now_utc = datetime.now(timezone.utc)
                            date_str = now_utc.strftime("%Y-%m-%d")
                            hour_str = now_utc.strftime("%H") if category == "playback" else None

                        buf_key = (category, date_str, hour_str)
                        if buf_key not in self.buffers:
                            self.buffers[buf_key] = []
                        self.buffers[buf_key].append(val)

                        if self.max_messages and msg_count >= self.max_messages:
                            break
                    if self.max_messages and msg_count >= self.max_messages:
                        break

                # Flush every 50 records or 3 seconds
                if now - last_flush >= 3.0:
                    self.flush_all()
                    last_flush = now

                if self.max_messages and msg_count >= self.max_messages:
                    print(f"Reached max messages target: {self.max_messages}")
                    break

                if self.timeout_seconds and (now - start_time) >= self.timeout_seconds:
                    print(f"Timeout reached ({self.timeout_seconds}s). Exiting lake writer.")
                    break

        except KeyboardInterrupt:
            print("\nLake writer stopped by operator.")
        finally:
            self.flush_all()
            self.consumer.close()
            print(f"Lake writer finished. Total records archived: {self.total_written}")

def main():
    max_m = int(sys.argv[1]) if len(sys.argv) > 1 else None
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    writer = ParquetLakeWriter(max_messages=max_m, timeout_seconds=timeout)
    writer.run()

if __name__ == "__main__":
    main()
