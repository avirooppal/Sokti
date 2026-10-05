#!/usr/bin/env python3
"""
Sokti OTT Platform - Debezium PostgreSQL Connector Registrar
Registers or updates the PostgreSQL Debezium CDC connector on Kafka Connect.
"""

import json
import os
import sys
import time
import requests
from dotenv import load_dotenv

load_dotenv()

CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = "sokti-postgres-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "sokti",
        "topic.prefix": "sokti_cdc",
        "table.include.list": "public.users,public.subscriptions,public.plans",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        "time.precision.mode": "connect",
        "snapshot.mode": "initial"
    }
}

def register_debezium_connector():
    print(f"Connecting to Kafka Connect at {CONNECT_URL}...")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # 1. Check existing connectors
    try:
        res = requests.get(f"{CONNECT_URL}/connectors", timeout=10)
        existing = res.json()
        print(f"Existing connectors: {existing}")
    except Exception as e:
        print(f"Failed to query Kafka Connect: {e}")
        sys.exit(1)

    # 2. Register or update connector
    if CONNECTOR_NAME in existing:
        print(f"Updating configuration for existing connector '{CONNECTOR_NAME}'...")
        put_res = requests.put(
            f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config",
            headers=headers,
            json=CONNECTOR_CONFIG["config"],
            timeout=10
        )
        if put_res.status_code not in [200, 201]:
            print(f"Failed to update connector: {put_res.text}")
            sys.exit(1)
    else:
        print(f"Creating new connector '{CONNECTOR_NAME}'...")
        post_res = requests.post(
            f"{CONNECT_URL}/connectors",
            headers=headers,
            json=CONNECTOR_CONFIG,
            timeout=10
        )
        if post_res.status_code not in [200, 201]:
            print(f"Failed to create connector: {post_res.text}")
            sys.exit(1)

    # 3. Wait for connector and task to reach RUNNING status
    print("Waiting for connector and tasks to enter RUNNING state...")
    for _ in range(15):
        time.sleep(2)
        stat_res = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=5)
        if stat_res.status_code == 200:
            status_data = stat_res.json()
            connector_state = status_data.get("connector", {}).get("state")
            tasks = status_data.get("tasks", [])
            task_states = [t.get("state") for t in tasks]
            print(f"Connector: {connector_state} | Tasks: {task_states}")
            if connector_state == "RUNNING" and all(s == "RUNNING" for s in task_states) and len(tasks) > 0:
                print(f"\nDebezium PostgreSQL connector '{CONNECTOR_NAME}' is ACTIVE and RUNNING!")
                return
    print("Connector registered, but not all tasks reached RUNNING within timeout.")

if __name__ == "__main__":
    register_debezium_connector()
