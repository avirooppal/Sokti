#!/usr/bin/env python3
"""
Sokti OTT Platform - Schema Registry Manager
Registers Avro schemas with Confluent Schema Registry.
"""

import json
import os
import sys
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

SUBJECT_MAPPINGS = {
    "ott.playback.events.v1-value": "playback_event.avsc",
    "ott.search.events.v1-value": "search_event.avsc",
    "ott.recommendation.events.v1-value": "recommendation_event.avsc",
    "ott.user.events.v1-value": "user_event.avsc",
    "ott.subscription.cdc.v1-value": "subscription_event.avsc"
}

def register_schemas():
    schemas_dir = Path(__file__).parent
    print(f"Connecting to Schema Registry at {SCHEMA_REGISTRY_URL}...")

    # Verify connectivity
    try:
        res = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
        if res.status_code != 200:
            print(f"Schema Registry returned HTTP {res.status_code}: {res.text}")
            sys.exit(1)
        print(f"Current registered subjects: {res.json()}")
    except Exception as e:
        print(f"Failed to reach Schema Registry: {e}")
        sys.exit(1)

    registered_count = 0
    for subject, schema_filename in SUBJECT_MAPPINGS.items():
        schema_path = schemas_dir / schema_filename
        if not schema_path.exists():
            print(f"Schema file {schema_filename} not found!")
            continue

        with open(schema_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)

        payload = {"schema": json.dumps(schema_json)}
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        reg_res = requests.post(
            f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
            headers=headers,
            json=payload,
            timeout=10
        )

        if reg_res.status_code in [200, 201]:
            schema_id = reg_res.json().get("id")
            print(f" -> Subject '{subject}' successfully registered (Schema ID: {schema_id})")
            registered_count += 1
        else:
            print(f" -> Failed to register '{subject}' (HTTP {reg_res.status_code}): {reg_res.text}")

    print(f"\nRegistered {registered_count}/{len(SUBJECT_MAPPINGS)} schemas in Schema Registry.")

if __name__ == "__main__":
    register_schemas()
