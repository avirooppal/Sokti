#!/usr/bin/env python3
"""
Sokti OTT Platform - ClickHouse Verifier & Query Utility
Queries ClickHouse raw_playback_events table and prints summary metrics.
"""

import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "sokti")

def execute_query(query_sql):
    query_clean = query_sql.strip().rstrip(";")
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {
        "query": f"{query_clean} FORMAT JSON",
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD
    }
    res = requests.get(url, params=params, timeout=10)
    if res.status_code != 200:
        raise RuntimeError(f"ClickHouse query failed (HTTP {res.status_code}): {res.text}")
    return res.json()

def main():
    print(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    
    # 1. Total row count
    count_res = execute_query(f"SELECT count(*) as total_rows FROM {CLICKHOUSE_DB}.raw_playback_events;")
    total_rows = count_res["data"][0]["total_rows"]
    print(f"\n=======================================================")
    print(f" Total rows in {CLICKHOUSE_DB}.raw_playback_events: {total_rows}")
    print(f"=======================================================")

    # 2. Aggregations by event_type
    agg_res = execute_query(f"""
        SELECT 
            event_type, 
            count(*) as event_count, 
            round(avg(position_seconds), 2) as avg_position_sec,
            round(avg(playback_seconds), 2) as avg_played_sec
        FROM {CLICKHOUSE_DB}.raw_playback_events
        GROUP BY event_type
        ORDER BY event_count DESC;
    """)
    print("\nEvent Distribution:")
    print(f"{'Event Type':<20} | {'Count':<8} | {'Avg Pos (s)':<12} | {'Avg Played (s)':<12}")
    print("-" * 60)
    for row in agg_res["data"]:
        print(f"{row['event_type']:<20} | {row['event_count']:<8} | {row['avg_position_sec']:<12} | {row['avg_played_sec']:<12}")

    # 3. Latest events preview
    sample_res = execute_query(f"""
        SELECT 
            event_id, 
            event_type, 
            content_id, 
            user_id, 
            position_seconds, 
            event_time 
        FROM {CLICKHOUSE_DB}.raw_playback_events 
        ORDER BY ingestion_time DESC 
        LIMIT 5;
    """)
    print("\nRecent 5 Sample Records:")
    for row in sample_res["data"]:
        print(f" - [{row['event_time']}] {row['event_type']} | Content: {row['content_id']} | User: {row['user_id'][:8]}... | Pos: {row['position_seconds']}s")

if __name__ == "__main__":
    main()
