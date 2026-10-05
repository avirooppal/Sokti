#!/usr/bin/env python3
"""
Sokti OTT Platform - Kafka Topic Provisioner
Creates topics with explicit partition counts, replication factors, and retention policies.
"""

import os
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_CONFIGS = [
    # High-volume streaming telemetry, partitioned by user_id for strict order per user
    {
        "name": "ott.playback.events.v1",
        "partitions": 6,
        "replication_factor": 1,
        "config": {"retention.ms": "604800000", "cleanup.policy": "delete"} # 7 days
    },
    # Search queries and click telemetry
    {
        "name": "ott.search.events.v1",
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": "604800000", "cleanup.policy": "delete"}
    },
    # Recommendation impressions & engagement
    {
        "name": "ott.recommendation.events.v1",
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": "604800000", "cleanup.policy": "delete"}
    },
    # User lifecycle events (login, watchlist adds)
    {
        "name": "ott.user.events.v1",
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": "1209600000", "cleanup.policy": "delete"} # 14 days
    },
    # Subscription status changes & CDC events
    {
        "name": "ott.subscription.cdc.v1",
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": "2592000000", "cleanup.policy": "compact"} # Compacted topic
    },
    # Dead Letter Queue for malformed, corrupt, or schema-invalid events
    {
        "name": "ott.playback.dlq.v1",
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": "1209600000", "cleanup.policy": "delete"}
    }
]

def create_topics():
    print(f"Connecting to Kafka AdminClient at {BOOTSTRAP_SERVERS}...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id="sokti_topic_provisioner"
        )
    except Exception as e:
        print(f"Failed to connect to Kafka AdminClient: {e}")
        sys.exit(1)

    existing_topics = set(admin_client.list_topics())
    print(f"Existing topics in cluster: {existing_topics}")

    new_topics = []
    for cfg in TOPIC_CONFIGS:
        tname = cfg["name"]
        if tname in existing_topics:
            print(f" -> Topic '{tname}' already exists.")
        else:
            new_topics.append(NewTopic(
                name=tname,
                num_partitions=cfg["partitions"],
                replication_factor=cfg["replication_factor"],
                topic_configs=cfg["config"]
            ))

    if new_topics:
        print(f"Creating {len(new_topics)} new topics...")
        try:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            print("Successfully created topics:")
            for t in new_topics:
                print(f"  + {t.name} (partitions: {t.num_partitions})")
        except Exception as e:
            print(f"Error creating topics: {e}")
    else:
        print("All required topics are already provisioned.")

    admin_client.close()

if __name__ == "__main__":
    create_topics()
