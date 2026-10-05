#!/usr/bin/env python3
"""
Sokti OTT Platform - Historical Event Replay Utility
Reads partitioned Parquet files from MinIO lake storage for a target date and replays events back to Kafka.
Usage:
  python data_lake/replay_events.py --date 2026-10-05
"""

import argparse
import io
import json
import os
import sys
import boto3
import pyarrow.parquet as pq
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
RAW_BUCKET = "sokti-raw"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1"
    )

def replay_events_for_date(target_date: str, target_topic=None, category=None):
    s3 = get_s3_client()
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )

    prefix = f"raw/{category}/" if category else "raw/"
    print(f"Scanning s3://{RAW_BUCKET}/{prefix} for partition date={target_date}...")

    paginator = s3.get_paginator("list_objects_v2")
    matched_keys = []
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if f"date={target_date}" in key and key.endswith(".parquet"):
                matched_keys.append(key)

    if not matched_keys:
        print(f"No Parquet files found for date={target_date} in bucket '{RAW_BUCKET}'.")
        return

    print(f"Found {len(matched_keys)} Parquet partition files to replay:")
    for k in matched_keys:
        print(f"  - {k}")

    total_replayed = 0
    for key in matched_keys:
        print(f"\nReading s3://{RAW_BUCKET}/{key}...")
        resp = s3.get_object(Bucket=RAW_BUCKET, Key=key)
        buffer = io.BytesIO(resp["Body"].read())
        table = pq.read_table(buffer)
        records = table.to_pylist()

        # Determine topic
        if target_topic:
            dest_topic = target_topic
        elif "playback" in key:
            dest_topic = "ott.playback.events.v1"
        elif "search" in key:
            dest_topic = "ott.search.events.v1"
        else:
            dest_topic = "ott.subscription.cdc.v1"

        print(f"Replaying {len(records)} events to topic '{dest_topic}'...")
        for rec in records:
            # Mark as replayed in metadata
            rec["is_replayed"] = True
            key_id = rec.get("user_id") or rec.get("event_id")
            producer.send(dest_topic, key=key_id, value=rec)
            total_replayed += 1

        producer.flush()

    producer.close()
    print(f"\n>>> Replay completed! Successfully replayed {total_replayed} historical events from {len(matched_keys)} files for date={target_date}. <<<")

def main():
    parser = argparse.ArgumentParser(description="Replay historical OTT events from Lake Parquet to Kafka")
    parser.add_argument("--date", type=str, required=True, help="Partition date to replay (YYYY-MM-DD)")
    parser.add_argument("--category", type=str, default=None, help="Filter category (playback, search, subscriptions)")
    parser.add_argument("--target-topic", type=str, default=None, help="Optional override topic to produce to")

    args = parser.parse_args()
    replay_events_for_date(target_date=args.date, target_topic=args.target_topic, category=args.category)

if __name__ == "__main__":
    main()
