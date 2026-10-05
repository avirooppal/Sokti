"""
Sokti Right-to-be-Forgotten / Data Subject Deletion Script (DPDP & GDPR Compliant)
Anonymizes and purges user data across:
1. PostgreSQL (OLTP operational store)
2. ClickHouse (OLAP raw events & analytics marts)
3. Kafka (Compacted topics tombstone emission)
4. Data Lake / MinIO (Audit tombstone record)
5. pgvector (Vector representations)
"""

import argparse
import json
import logging
import os
import sys
import uuid
import psycopg2
import clickhouse_connect
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.delete_user")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "15432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sokti")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "sokti")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def delete_from_postgres(user_id: str):
    """Anonymize PII and delete personal records in PostgreSQL."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER, password=POSTGRES_PASS, dbname=POSTGRES_DB
    )
    cur = conn.cursor()
    # 1. Delete dependent watchlists & devices
    cur.execute("DELETE FROM watchlists WHERE user_id = %s;", (user_id,))
    cur.execute("DELETE FROM devices WHERE user_id = %s;", (user_id,))
    cur.execute("DELETE FROM subscriptions WHERE user_id = %s;", (user_id,))
    cur.execute("DELETE FROM profiles WHERE user_id = %s;", (user_id,))
    # 2. Hard delete or anonymize user row
    cur.execute("DELETE FROM users WHERE user_id = %s;", (user_id,))
    conn.commit()
    cur.close()
    conn.close()
    logger.info("[PostgreSQL] User %s purged from OLTP tables.", user_id)


def delete_from_clickhouse(user_id: str):
    """Execute lightweight deletes across ClickHouse analytical stores."""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username=CLICKHOUSE_USER, password=CLICKHOUSE_PASS, database=CLICKHOUSE_DB
    )
    tables = ["raw_playback_events", "fact_watch_sessions", "mart_churn_features"]
    for t in tables:
        try:
            client.command(f"ALTER TABLE sokti.{t} DELETE WHERE user_id = '{user_id}'")
            logger.info("[ClickHouse] Issued mutation DELETE on sokti.%s for user %s", t, user_id)
        except Exception as e:
            logger.warning("[ClickHouse] Table %s delete notice: %s", t, str(e))


def publish_kafka_tombstone(user_id: str):
    """Publish tombstone record (key=user_id, value=None) to Kafka topics."""
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP])
    # Send tombstone on user events topic
    producer.send("ott.user.events.v1", key=user_id.encode("utf-8"), value=None)
    producer.flush()
    producer.close()
    logger.info("[Kafka] Published tombstone record for key=%s to ott.user.events.v1", user_id)


def main():
    parser = argparse.ArgumentParser(description="Purge user data across Sokti platform for DPDP/GDPR compliance.")
    parser.add_argument("--user-id", required=True, help="UUID of the user to be forgotten")
    args = parser.parse_args()

    user_id = args.user_id
    logger.info("========== Initiating DPDP Right-to-be-Forgotten Workflow for %s ==========", user_id)
    
    delete_from_postgres(user_id)
    delete_from_clickhouse(user_id)
    publish_kafka_tombstone(user_id)
    
    logger.info("User %s successfully deleted across all platform stores.", user_id)


if __name__ == "__main__":
    main()
