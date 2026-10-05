#!/usr/bin/env python3
"""
Sokti OTT Platform - ClickHouse Table Design Benchmark
Compares query performance and compression between:
1. Poor Table Design: No partitioning, UUID primary key order, plain String types.
2. Optimized Table Design: Monthly partitioning, compound primary key (event_type, user_id, event_time), LowCardinality columns.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CH_DB = os.getenv("CLICKHOUSE_DB", "sokti")

def execute_sql(sql):
    url = f"http://{CH_HOST}:{CH_PORT}/"
    params = {"query": sql, "user": CH_USER, "password": CH_PASS}
    res = requests.post(url, params=params, timeout=30)
    if res.status_code != 200:
        raise RuntimeError(f"ClickHouse query failed: {res.text}")
    return res.text

def query_json(sql):
    clean_sql = sql.strip().rstrip(";")
    url = f"http://{CH_HOST}:{CH_PORT}/"
    params = {"query": f"{clean_sql} FORMAT JSON", "user": CH_USER, "password": CH_PASS}
    res = requests.get(url, params=params, timeout=30)
    if res.status_code != 200:
        raise RuntimeError(f"ClickHouse query failed: {res.text}")
    return res.json()

def setup_benchmark_tables():
    print("Creating benchmark tables in ClickHouse...")
    
    # 1. Poor Design: No partition, unindexed random UUID order, plain Strings
    execute_sql(f"""
    CREATE TABLE IF NOT EXISTS {CH_DB}.bench_poor_playback (
        event_id UUID,
        event_type String,
        schema_version String,
        user_id UUID,
        session_id UUID,
        device_id UUID,
        device_type String,
        app_version String,
        content_id String,
        position_seconds Float32,
        playback_seconds Float32,
        event_time DateTime64(3, 'UTC'),
        ingestion_time DateTime64(3, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY event_id;
    """)

    # 2. Optimized Design: Partitioned by month, compound key, LowCardinality columns
    execute_sql(f"""
    CREATE TABLE IF NOT EXISTS {CH_DB}.bench_optimized_playback (
        event_id UUID,
        event_type LowCardinality(String),
        schema_version LowCardinality(String),
        user_id UUID,
        session_id UUID,
        device_id UUID,
        device_type LowCardinality(String),
        app_version LowCardinality(String),
        content_id String,
        position_seconds Float32,
        playback_seconds Float32,
        event_time DateTime64(3, 'UTC'),
        ingestion_time DateTime64(3, 'UTC')
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(event_time)
    ORDER BY (event_type, user_id, event_time, event_id);
    """)

def populate_benchmark_data(num_rows=100000):
    # Check if already populated
    cnt_poor = int(query_json(f"SELECT count(*) as c FROM {CH_DB}.bench_poor_playback")["data"][0]["c"])
    cnt_opt = int(query_json(f"SELECT count(*) as c FROM {CH_DB}.bench_optimized_playback")["data"][0]["c"])

    if cnt_poor >= num_rows and cnt_opt >= num_rows:
        print(f"Benchmark tables already contain >= {num_rows} rows. Proceeding to tests.")
        return

    print(f"Generating {num_rows} synthetic benchmark records...")
    now = datetime.now(timezone.utc)
    user_pool = [str(uuid.uuid4()) for _ in range(5000)]
    content_pool = [f"cnt_mov_{i:04d}" for i in range(1, 101)]
    event_types = ["video_play", "video_pause", "video_seek", "video_stop", "video_complete"]
    device_types = ["smart_tv", "mobile", "tablet", "web", "streaming_stick"]

    batch_size = 20000
    for b in range(0, num_rows, batch_size):
        rows = []
        for _ in range(batch_size):
            u_id = random.choice(user_pool)
            days_ago = random.randint(0, 45)
            etime = now - timedelta(days=days_ago, seconds=random.randint(0, 86400))
            etime_str = etime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            pos = random.uniform(0, 7200)
            rows.append({
                "event_id": str(uuid.uuid4()),
                "event_type": random.choice(event_types),
                "schema_version": "1.0.0",
                "user_id": u_id,
                "session_id": str(uuid.uuid4()),
                "device_id": str(uuid.uuid4()),
                "device_type": random.choice(device_types),
                "app_version": "3.12.0",
                "content_id": random.choice(content_pool),
                "position_seconds": round(pos, 2),
                "playback_seconds": round(random.uniform(10, 300), 2),
                "event_time": etime_str,
                "ingestion_time": etime_str
            })

        payload = "\n".join([json.dumps(r) for r in rows]) + "\n"
        url = f"http://{CH_HOST}:{CH_PORT}/"

        # Insert into poor table
        p1 = {"query": f"INSERT INTO {CH_DB}.bench_poor_playback FORMAT JSONEachRow", "user": CH_USER, "password": CH_PASS}
        requests.post(url, params=p1, data=payload, timeout=30)

        # Insert into optimized table
        p2 = {"query": f"INSERT INTO {CH_DB}.bench_optimized_playback FORMAT JSONEachRow", "user": CH_USER, "password": CH_PASS}
        requests.post(url, params=p2, data=payload, timeout=30)

        print(f" -> Inserted batch {b + batch_size}/{num_rows} rows.")

def run_benchmark_queries():
    queries = [
        {
            "name": "Q1: Filter by Event Type & Time Window",
            "sql_template": """
                SELECT count(*), round(avg(playback_seconds), 2) as avg_played
                FROM {table}
                WHERE event_type = 'video_play'
                  AND event_time >= now() - INTERVAL 14 DAY
            """
        },
        {
            "name": "Q2: Device Type Breakdown & Watch Time Aggregation",
            "sql_template": """
                SELECT device_type, count(*) as event_cnt, round(sum(playback_seconds), 2) as total_watch
                FROM {table}
                GROUP BY device_type
                ORDER BY total_watch DESC
            """
        },
        {
            "name": "Q3: Top 10 Most Active Users",
            "sql_template": """
                SELECT user_id, count(*) as play_count, sum(playback_seconds) as total_seconds
                FROM {table}
                WHERE event_type = 'video_play'
                GROUP BY user_id
                ORDER BY total_seconds DESC
                LIMIT 10
            """
        }
    ]

    results = []
    iterations = 5

    print("\nRunning benchmarks (5 runs per query)...")
    for q in queries:
        qname = q["name"]
        print(f"\n--- {qname} ---")

        # 1. Poor Design
        sql_poor = q["sql_template"].format(table=f"{CH_DB}.bench_poor_playback")
        times_poor = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            query_json(sql_poor)
            times_poor.append((time.perf_counter() - t0) * 1000)
        avg_poor = sum(times_poor) / len(times_poor)

        # 2. Optimized Design
        sql_opt = q["sql_template"].format(table=f"{CH_DB}.bench_optimized_playback")
        times_opt = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            query_json(sql_opt)
            times_opt.append((time.perf_counter() - t0) * 1000)
        avg_opt = sum(times_opt) / len(times_opt)

        speedup = avg_poor / max(avg_opt, 0.001)
        print(f"  Poor Design      : {avg_poor:.2f} ms")
        print(f"  Optimized Design : {avg_opt:.2f} ms (Speedup: {speedup:.2f}x)")

        results.append({
            "query": qname,
            "poor_ms": round(avg_poor, 2),
            "opt_ms": round(avg_opt, 2),
            "speedup": round(speedup, 2)
        })

    # Storage Footprint Comparison
    size_query = f"""
    SELECT
        table,
        formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
        formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
        round(sum(data_uncompressed_bytes) / greatest(sum(data_compressed_bytes), 1), 2) as compression_ratio
    FROM system.parts
    WHERE database = '{CH_DB}' AND table IN ('bench_poor_playback', 'bench_optimized_playback') AND active
    GROUP BY table;
    """
    sizes = query_json(size_query)["data"]
    print("\n--- Storage Compression Footprint ---")
    for s in sizes:
        print(f" Table: {s['table']} | Compressed: {s['compressed_size']} | Compression Ratio: {s['compression_ratio']}x")

    save_benchmark_report(results, sizes)

def save_benchmark_report(results, sizes):
    doc_path = Path(__file__).resolve().parent.parent.parent / "docs" / "clickhouse_benchmarks.md"
    
    md_content = f"""# ClickHouse Performance Benchmark: Schema Design Impact

This benchmark compares a **poorly designed table** against a **production-optimized ClickHouse table** operating over high-cardinality streaming OTT playback telemetry (100,000 rows).

---

## 1. Architectural Schema Differences

| Feature | Poor Table Design (`bench_poor_playback`) | Optimized Table Design (`bench_optimized_playback`) | Engineering Impact |
| :--- | :--- | :--- | :--- |
| **Partitioning** | None | `PARTITION BY toYYYYMM(event_time)` | Partition pruning skips entire months of data for date-filtered queries. |
| **Primary Key / Order**| `ORDER BY event_id` (Random UUID) | `ORDER BY (event_type, user_id, event_time, event_id)` | Locality sorting groups identical events and users together in sparse indexes. |
| **String Columns** | Plain `String` for all text fields | `LowCardinality(String)` for enum-like categories | Replaces full string storage with integer dictionary references (1-2 bytes). |
| **Index Granularity**| Granule skipping fails due to high entropy | Granule skipping leverages sorted sparse index | Minimizes data scanned from disk/memory by orders of magnitude. |

---

## 2. Query Latency Benchmark Results

Evaluated with 5 iterations per query; latencies reported in milliseconds:

| Query Scenario | Poor Design (ms) | Optimized Design (ms) | Performance Gain |
| :--- | :--- | :--- | :--- |
"""
    for r in results:
        md_content += f"| **{r['query']}** | {r['poor_ms']} ms | {r['opt_ms']} ms | **{r['speedup']}x faster** |\n"

    md_content += """
---

## 3. Storage Compression & Memory Footprint

| Table Name | Compressed Size | Uncompressed Size | Compression Ratio |
| :--- | :--- | :--- | :--- |
"""
    for s in sizes:
        md_content += f"| `{s['table']}` | {s['compressed_size']} | {s['uncompressed_size']} | **{s['compression_ratio']}x** |\n"

    md_content += """
---

## 4. Key Takeaways

1. **Avoid UUID as the sole primary key**: UUID primary keys destroy ClickHouse's sparse indexing efficiency because adjacent granules share no similarity.
2. **Leverage `LowCardinality(String)`**: Categorical fields such as `event_type`, `device_type`, and `app_version` achieve dramatic compression improvements when converted to dictionary encodings.
3. **Align `ORDER BY` with query access patterns**: Placing high-frequency filter columns (`event_type`, `user_id`) at the front of the composite sort key enables ClickHouse to skip non-matching granules instantly.
"""

    with open(doc_path, "w", encoding="utf-8") as f:
        f.write(md_content)
    print(f"\nSaved benchmark documentation to {doc_path}")

def main():
    setup_benchmark_tables()
    populate_benchmark_data(num_rows=100000)
    run_benchmark_queries()

if __name__ == "__main__":
    main()
