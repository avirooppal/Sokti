"""
Sokti Core OTT Platform API
Handles user profile fetching with DPDP PII masking, content metadata discovery, and real-time event ingestion into Kafka.
"""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from kafka import KafkaProducer

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from governance.masking.pii_masker import mask_email, mask_phone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.core_api")

app = FastAPI(
    title="Sokti Core Platform API",
    version="1.0.0",
    description="Operational Gateway for user profiles, content metadata, and event ingestion.",
)

# Database configs
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "15432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sokti")

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27018"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
MONGO_PASS = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin")
MONGO_DB = os.getenv("MONGO_DATABASE", "sokti_metadata")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PLAYBACK_TOPIC = "ott.playback.events.v1"

# Producer lazy singleton
_producer: Optional[KafkaProducer] = None


def get_kafka_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )
    return _producer


class PlaybackEventPayload(BaseModel):
    user_id: str
    session_id: str
    device_id: str
    device_type: str = "smart_tv"
    app_version: str = "3.4.1"
    content_id: str
    event_type: str = Field(..., example="video_play")
    position_seconds: float = 0.0
    playback_seconds: float = 0.0


@app.get("/health")
def health():
    return {"status": "ok", "service": "sokti-core-api"}


@app.get("/api/v1/users/{user_id}")
def get_user_profile(user_id: str, mask_pii: bool = True):
    """Retrieve user details and subscription status with DPDP PII protection."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER, password=POSTGRES_PASS, dbname=POSTGRES_DB
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT u.user_id, u.email, u.phone, u.country_code, u.created_at,
                   s.status as subscription_status, p.plan_name, s.auto_renew
            FROM users u
            LEFT JOIN subscriptions s ON u.user_id = s.user_id
            LEFT JOIN plans p ON s.plan_id = p.plan_id
            WHERE u.user_id = %s;
        """, (user_id,))
        user_row = cur.fetchone()
        cur.close()
        conn.close()

        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")

        # Apply DPDP masking
        if mask_pii:
            user_row["email"] = mask_email(user_row.get("email"))
            user_row["phone"] = mask_phone(user_row.get("phone"))

        return user_row
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error retrieving user: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/content/{content_id}")
def get_content_metadata(content_id: str):
    """Retrieve complete metadata for a movie or series from MongoDB."""
    try:
        client = MongoClient(
            f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
        )
        db = client[MONGO_DB]
        doc = db["movies"].find_one({"content_id": content_id}, {"_id": 0})
        if not doc:
            doc = db["series"].find_one({"content_id": content_id}, {"_id": 0})
        
        if not doc:
            raise HTTPException(status_code=404, detail="Content not found")
        return doc
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error retrieving content: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/events/playback", status_code=status.HTTP_202_ACCEPTED)
def ingest_playback_event(payload: PlaybackEventPayload):
    """Ingest playback activity into streaming Kafka pipeline."""
    try:
        producer = get_kafka_producer()
        now_str = datetime.now(timezone.utc).isoformat()
        event_id = str(uuid.uuid4())

        event_dict = {
            "event_id": event_id,
            "event_type": payload.event_type,
            "schema_version": "1.0.0",
            "user_id": payload.user_id,
            "session_id": payload.session_id,
            "device_id": payload.device_id,
            "device_type": payload.device_type,
            "app_version": payload.app_version,
            "content_id": payload.content_id,
            "position_seconds": payload.position_seconds,
            "playback_seconds": payload.playback_seconds,
            "event_time": now_str,
            "ingestion_time": now_str,
        }

        producer.send(
            PLAYBACK_TOPIC,
            key=payload.user_id.encode("utf-8"),
            value=event_dict,
        )
        return {"status": "accepted", "event_id": event_id, "topic": PLAYBACK_TOPIC}
    except Exception as e:
        logger.error("Error ingesting playback event: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
