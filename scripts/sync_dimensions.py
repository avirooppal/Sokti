#!/usr/bin/env python3
"""
Sokti OTT Platform - Dimension Sync Utility
Synchronizes content metadata from MongoDB and device profiles from PostgreSQL into ClickHouse dimension tables.
"""

import json
import os
import psycopg2
from pymongo import MongoClient
import requests
from dotenv import load_dotenv

load_dotenv()

# Postgres
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "15432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "sokti")

# Mongo
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27018"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
MONGO_PASS = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin")
MONGO_DB = os.getenv("MONGO_DATABASE", "sokti_metadata")

# ClickHouse
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CH_DB = os.getenv("CLICKHOUSE_DB", "sokti")

def ch_insert(table, rows):
    if not rows:
        return
    url = f"http://{CH_HOST}:{CH_PORT}/"
    params = {
        "query": f"INSERT INTO {CH_DB}.{table} FORMAT JSONEachRow",
        "user": CH_USER,
        "password": CH_PASS
    }
    payload = "\n".join([json.dumps(r) for r in rows]) + "\n"
    res = requests.post(url, params=params, data=payload, timeout=10)
    if res.status_code != 200:
        raise RuntimeError(f"ClickHouse insert failed: {res.text}")
    print(f" -> Inserted {len(rows)} records into {CH_DB}.{table}")

def sync_dim_content():
    print("Syncing dim_content from MongoDB...")
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
    client = MongoClient(uri)
    db = client[MONGO_DB]

    docs = list(db.content_metadata.find())
    rows = []
    for d in docs:
        rows.append({
            "content_id": d["content_id"],
            "title": d.get("title", "Untitled"),
            "genres": d.get("genres", ["General"]),
            "language": d.get("language", "en"),
            "release_year": int(d.get("release_year", 2024)),
            "director": d.get("director", "Unknown"),
            "duration_seconds": int(d.get("duration_seconds", 5400)),
            "maturity_rating": d.get("maturity_rating", "U/A 13+")
        })
    ch_insert("dim_content", rows)

def sync_dim_device():
    print("Syncing dim_device from PostgreSQL...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    with conn.cursor() as cur:
        cur.execute("SELECT device_id, device_type, device_name, os, app_version FROM devices;")
        rows = [
            {
                "device_id": str(r[0]),
                "device_type": r[1],
                "device_name": r[2],
                "os": r[3] or "Unknown",
                "app_version": r[4] or "1.0.0"
            }
            for r in cur.fetchall()
        ]
    conn.close()
    ch_insert("dim_device", rows)

def main():
    sync_dim_content()
    sync_dim_device()
    print("Dimension synchronization complete!")

if __name__ == "__main__":
    main()
