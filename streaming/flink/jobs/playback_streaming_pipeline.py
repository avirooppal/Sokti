#!/usr/bin/env python3
"""
Sokti OTT Platform - Streaming Pipeline Engine
Performs stream ingestion from Kafka:
1. Validation against schema
2. Deduplication by event_id
3. Event-time watermarking and late-arrival handling
4. Dead-Letter Queue (DLQ) routing for corrupt/invalid events
5. Content metadata enrichment
6. Stateful sessionization (watch_seconds, pause_count, seek_count, completion_percentage)
7. Tumbling window aggregations
8. Sink delivery to ClickHouse
"""

import json
import os
import sys
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
import requests
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SOURCE_TOPIC = "ott.playback.events.v1"
DLQ_TOPIC = "ott.playback.dlq.v1"

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CH_DB = os.getenv("CLICKHOUSE_DB", "sokti")

ALLOWED_LATENESS_SECONDS = 30.0

class StreamingPipeline:
    def __init__(self, consumer_group="sokti-playback-streaming-engine",
                 max_events=None, timeout_seconds=None):
        self.max_events = max_events
        self.timeout_seconds = timeout_seconds

        # Deduplication cache (LRU set of event_id)
        self.seen_event_ids = OrderedDict()
        self.dedup_cache_size = 50000

        # High watermark tracker for event-time processing
        self.max_event_time: Optional[datetime] = None
        self.current_watermark: Optional[datetime] = None

        # Content metadata cache: {content_id: {"title": ..., "duration_seconds": ...}}
        self.content_cache: Dict[str, Dict[str, Any]] = {}
        self._load_content_metadata()

        # Session state: {(user_id, session_id): {...}}
        self.active_sessions: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # Tumbling window state: {window_start_iso: {"events": 0, "plays_by_content": {}, "watch_seconds_by_content": {}}}
        self.tumbling_windows: Dict[str, Dict[str, Any]] = {}

        # Metrics
        self.processed_count = 0
        self.dedup_dropped_count = 0
        self.dlq_count = 0
        self.late_event_count = 0

        self.producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None
        )

        self.consumer = KafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            group_id=consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000
        )

    def _load_content_metadata(self):
        """Pre-populate content catalog cache from ClickHouse dim_content."""
        try:
            url = f"http://{CH_HOST}:{CH_PORT}/"
            params = {
                "query": f"SELECT content_id, title, duration_seconds FROM {CH_DB}.dim_content FORMAT JSON",
                "user": CH_USER, "password": CH_PASS
            }
            res = requests.get(url, params=params, timeout=5)
            if res.status_code == 200:
                data = res.json().get("data", [])
                for row in data:
                    self.content_cache[row["content_id"]] = {
                        "title": row["title"],
                        "duration_seconds": float(row.get("duration_seconds", 7200))
                    }
                print(f"[StreamingPipeline] Preloaded {len(self.content_cache)} content items into metadata cache.")
        except Exception as e:
            print(f"[StreamingPipeline] Warning: Could not preload content cache ({e}). Using dynamic defaults.")

    def _validate_event(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validates payload against schema requirements."""
        if not isinstance(event, dict):
            return False, "Payload is not a valid JSON dictionary"

        required_keys = ["event_id", "event_type", "user_id", "session_id", "content_id", "event_time"]
        for key in required_keys:
            if key not in event or event[key] is None or event[key] == "":
                return False, f"Missing required field: '{key}'"

        valid_types = {"video_play", "video_pause", "video_seek", "video_stop", "video_complete"}
        if event["event_type"] not in valid_types:
            return False, f"Invalid event_type: '{event['event_type']}'"

        # Check timestamp format and future bounds
        try:
            etime = self._parse_event_time(event["event_time"])
            now_utc = datetime.now(timezone.utc)
            if etime > now_utc + timedelta(hours=2):
                return False, f"Event timestamp is too far in future: {event['event_time']}"
        except Exception as ex:
            return False, f"Invalid event_time format: {ex}"

        return True, None

    def _parse_event_time(self, t_str: str) -> datetime:
        # Supports ISO-8601 or YYYY-MM-DD HH:MM:SS.mmm
        t_clean = t_str.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(t_clean)
        except ValueError:
            dt = datetime.strptime(t_str[:19], "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _is_duplicate(self, event_id: str) -> bool:
        if event_id in self.seen_event_ids:
            return True
        self.seen_event_ids[event_id] = True
        if len(self.seen_event_ids) > self.dedup_cache_size:
            self.seen_event_ids.popitem(last=False)
        return False

    def _route_to_dlq(self, raw_event: Any, reason: str):
        self.dlq_count += 1
        dlq_payload = {
            "failed_event": raw_event,
            "failure_reason": reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "source_topic": SOURCE_TOPIC
        }
        self.producer.send(DLQ_TOPIC, key=str(uuid.uuid4()), value=dlq_payload)
        print(f" [DLQ] Routed event to {DLQ_TOPIC}: {reason}")

    def _update_watermark(self, event_time: datetime) -> bool:
        """
        Updates event-time watermark.
        Returns True if event is on-time, False if late-arriving past watermark.
        """
        if self.max_event_time is None or event_time > self.max_event_time:
            self.max_event_time = event_time
            self.current_watermark = event_time - timedelta(seconds=ALLOWED_LATENESS_SECONDS)

        if self.current_watermark and event_time < self.current_watermark:
            self.late_event_count += 1
            return False # Late event
        return True

    def _update_session_aggregate(self, event: Dict[str, Any]):
        key = (event["user_id"], event["session_id"])
        etime = self._parse_event_time(event["event_time"])
        content_id = event["content_id"]
        meta = self.content_cache.get(content_id, {"duration_seconds": 7200})
        total_duration = meta["duration_seconds"]

        if key not in self.active_sessions:
            self.active_sessions[key] = {
                "user_id": event["user_id"],
                "session_id": event["session_id"],
                "content_id": content_id,
                "device_type": event.get("device_type", "smart_tv"),
                "app_version": event.get("app_version", "1.0.0"),
                "watch_seconds": 0.0,
                "pause_count": 0,
                "seek_count": 0,
                "completion_percentage": 0.0,
                "session_start": etime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "session_end": etime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            }

        sess = self.active_sessions[key]
        sess["session_end"] = etime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        etype = event["event_type"]
        if etype == "video_pause":
            sess["pause_count"] += 1
        elif etype == "video_seek":
            sess["seek_count"] += 1
        elif etype == "video_play":
            sess["watch_seconds"] += float(event.get("playback_seconds", 30.0))

        pos = float(event.get("position_seconds", 0.0))
        sess["completion_percentage"] = min(100.0, round((pos / max(total_duration, 1.0)) * 100.0, 2))

    def _update_tumbling_windows(self, event: Dict[str, Any]):
        """Maintains 1-minute tumbling window metrics."""
        etime = self._parse_event_time(event["event_time"])
        window_start = etime.replace(second=0, microsecond=0).isoformat()
        content_id = event["content_id"]

        if window_start not in self.tumbling_windows:
            self.tumbling_windows[window_start] = {
                "events_count": 0,
                "plays_by_content": {},
                "watch_seconds_by_content": {}
            }

        win = self.tumbling_windows[window_start]
        win["events_count"] += 1

        if event["event_type"] == "video_play":
            win["plays_by_content"][content_id] = win["plays_by_content"].get(content_id, 0) + 1
            watch_sec = float(event.get("playback_seconds", 0.0))
            win["watch_seconds_by_content"][content_id] = win["watch_seconds_by_content"].get(content_id, 0.0) + watch_sec

    def flush_to_clickhouse(self, raw_events: List[Dict[str, Any]]):
        if not raw_events:
            return

        # 1. Insert raw events
        url = f"http://{CH_HOST}:{CH_PORT}/"
        params_raw = {
            "query": f"INSERT INTO {CH_DB}.raw_playback_events FORMAT JSONEachRow",
            "user": CH_USER, "password": CH_PASS
        }
        payload_raw = "\n".join([json.dumps(r) for r in raw_events]) + "\n"
        res_r = requests.post(url, params=params_raw, data=payload_raw, timeout=10)
        if res_r.status_code != 200:
            raise RuntimeError(f"ClickHouse raw insert failed: {res_r.text}")

        # 2. Insert session aggregates
        sessions_to_flush = list(self.active_sessions.values())
        if sessions_to_flush:
            params_sess = {
                "query": f"INSERT INTO {CH_DB}.fact_watch_sessions FORMAT JSONEachRow",
                "user": CH_USER, "password": CH_PASS
            }
            payload_sess = "\n".join([json.dumps(s) for s in sessions_to_flush]) + "\n"
            res_s = requests.post(url, params=params_sess, data=payload_sess, timeout=10)
            if res_s.status_code != 200:
                raise RuntimeError(f"ClickHouse session aggregate insert failed: {res_s.text}")

        print(f" [Sink] Committed {len(raw_events)} events and {len(sessions_to_flush)} sessions to ClickHouse.")

    def run(self):
        print(f"Starting Flink/Streaming Pipeline Engine on topic '{SOURCE_TOPIC}'...")
        start_time = time.time()
        batch_events = []
        batch_flush_interval = 2.0
        last_flush = time.time()

        try:
            while True:
                msg_dict = self.consumer.poll(timeout_ms=1000)
                now = time.time()

                if not msg_dict:
                    if self.timeout_seconds and (now - start_time) >= self.timeout_seconds:
                        print(f"Timeout reached ({self.timeout_seconds}s). Exiting streaming loop.")
                        break
                    continue

                for tp, messages in msg_dict.items():
                    for message in messages:
                        raw_event = message.value
                        self.processed_count += 1

                        # 1. Validation
                        valid, reason = self._validate_event(raw_event)
                        if not valid:
                            self._route_to_dlq(raw_event, reason)
                            continue

                        # 2. Deduplication by event_id
                        event_id = raw_event["event_id"]
                        if self._is_duplicate(event_id):
                            self.dedup_dropped_count += 1
                            continue

                        # 3. Watermarking & Event-time handling
                        event_time = self._parse_event_time(raw_event["event_time"])
                        is_on_time = self._update_watermark(event_time)
                        if not is_on_time:
                            raw_event["is_late"] = True

                        # 4. Enrichment
                        content_id = raw_event["content_id"]
                        meta = self.content_cache.get(content_id, {"title": "Unknown Title", "duration_seconds": 7200})
                        raw_event["content_title"] = meta["title"]

                        # 5. Stateful Sessionization & Tumbling Windows
                        self._update_session_aggregate(raw_event)
                        self._update_tumbling_windows(raw_event)

                        batch_events.append(raw_event)

                        if self.max_events and self.processed_count >= self.max_events:
                            break
                    if self.max_events and self.processed_count >= self.max_events:
                        break

                # Periodic or batch flush
                if len(batch_events) >= 100 or (now - last_flush >= batch_flush_interval and batch_events):
                    self.flush_to_clickhouse(batch_events)
                    batch_events = []
                    last_flush = now

                if self.max_events and self.processed_count >= self.max_events:
                    print(f"Reached max events limit: {self.max_events}")
                    break

                if self.timeout_seconds and (now - start_time) >= self.timeout_seconds:
                    print(f"Timeout reached ({self.timeout_seconds}s). Exiting streaming loop.")
                    break

        except KeyboardInterrupt:
            print("\nStreaming pipeline stopped by operator.")
        finally:
            if batch_events:
                self.flush_to_clickhouse(batch_events)
            self.producer.flush()
            self.producer.close()
            self.consumer.close()
            print(f"\nPipeline Summary: Processed: {self.processed_count} | Dedup Dropped: {self.dedup_dropped_count} | DLQ: {self.dlq_count} | Late: {self.late_event_count}")

def main():
    max_ev = int(sys.argv[1]) if len(sys.argv) > 1 else None
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else None
    pipeline = StreamingPipeline(max_events=max_ev, timeout_seconds=timeout)
    pipeline.run()

if __name__ == "__main__":
    main()
