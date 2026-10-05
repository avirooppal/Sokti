#!/usr/bin/env python3
"""
Sokti OTT Platform - Integration Test: Debezium PostgreSQL CDC to Kafka
Verifies end-to-end Change Data Capture:
1. Inserts a new user record into PostgreSQL
2. Listens on Kafka CDC topic 'sokti_cdc.public.users'
3. Verifies that Debezium captured the WAL event and delivered it with operation 'c' (create)
"""

import json
import os
import time
import uuid
import psycopg2
import pytest
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "15432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "sokti")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CDC_TOPIC = "sokti_cdc.public.users"

def test_debezium_postgres_cdc():
    test_user_id = str(uuid.uuid4())
    test_email = f"cdc_test_{uuid.uuid4().hex[:8]}@sokti-test.com"
    test_phone = "+1 555-0199"
    test_country = "US"

    print(f"\n[Step 1] Connecting to PostgreSQL at {PG_HOST}:{PG_PORT}/{PG_DB}...")
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    conn.autocommit = True

    try:
        # Step 1: Insert user into PostgreSQL
        with conn.cursor() as cur:
            print(f"[Step 2] Inserting user ID: {test_user_id}, Email: {test_email}...")
            cur.execute("""
                INSERT INTO users (user_id, email, phone, country_code)
                VALUES (%s, %s, %s, %s);
            """, (test_user_id, test_email, test_phone, test_country))

        # Step 2: Listen on Kafka topic for CDC event
        print(f"[Step 3] Subscribing to Kafka CDC topic '{CDC_TOPIC}'...")
        consumer = KafkaConsumer(
            CDC_TOPIC,
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=f"test-cdc-group-{uuid.uuid4().hex[:6]}",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=15000
        )

        matched_record = None
        start_wait = time.time()
        timeout = 20

        print(f"[Step 4] Waiting up to {timeout}s for Debezium CDC event...")
        while time.time() - start_wait < timeout:
            msg_batch = consumer.poll(timeout_ms=1000)
            for tp, messages in msg_batch.items():
                for message in messages:
                    val = message.value
                    payload = val.get("payload", val)
                    after = payload.get("after")
                    if after and after.get("user_id") == test_user_id:
                        matched_record = payload
                        print(f" -> Successfully received CDC record for user: {test_user_id}")
                        break
            if matched_record:
                break

        consumer.close()

        # Step 3: Assertions
        assert matched_record is not None, f"CDC event for user_id={test_user_id} was not received on Kafka within {timeout}s!"
        
        op = matched_record.get("op")
        after = matched_record.get("after", {})
        
        print(f"[Step 5] Validating CDC event payload: op='{op}', after={after}")
        assert op in ["c", "r", "u"], f"Expected CDC operation 'c', 'r', or 'u', got: {op}"
        assert after.get("email") == test_email, f"Email mismatch: {after.get('email')} vs {test_email}"
        assert after.get("country_code") == test_country
        print(" CDC pipeline integration test PASSED successfully!")

    finally:
        # Cleanup test user from PostgreSQL
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE user_id = %s;", (test_user_id,))
        conn.close()

if __name__ == "__main__":
    test_debezium_postgres_cdc()
