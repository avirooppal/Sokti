#!/usr/bin/env python3
"""
Sokti OTT Platform - Production Event Generator & Simulator
Simulates realistic streaming activities: playback state machines (play/pause/seek/complete),
searches, recommendation rails, and user lifecycle events.
Supports configurable traffic spikes and deliberate anomaly/invalid-event injection.
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_PLAYBACK = "ott.playback.events.v1"
TOPIC_SEARCH = "ott.search.events.v1"
TOPIC_RECOMMENDATION = "ott.recommendation.events.v1"
TOPIC_USER = "ott.user.events.v1"

SEARCH_QUERIES = [
    "Christopher Nolan sci-fi", "Hindi thrillers 2023", "Korean drama romance",
    "cyberpunk action", "investigative journalism", "family comedy",
    "astronauts deep space", "superhero origin", "monsoon acoustic melody",
    "horror abandoned radio", "mountain climbing documentary"
]

DEVICE_TYPES = ["smart_tv", "mobile", "tablet", "web", "streaming_stick"]
APP_VERSIONS = ["3.12.0", "3.12.1", "3.13.0-beta", "3.11.4"]

class EventGenerator:
    def __init__(self, num_users=1000, eps=100, invalid_rate=0.0,
                 spike_interval=30, spike_multiplier=3.0):
        self.num_users = num_users
        self.base_eps = eps
        self.invalid_rate = invalid_rate
        self.spike_interval = spike_interval
        self.spike_multiplier = spike_multiplier

        # Pre-seed user pool & session tracking
        self.user_ids = [str(uuid.uuid4()) for _ in range(num_users)]
        self.content_pool = [f"cnt_mov_{i:04d}" for i in range(1, 101)] + [f"cnt_ser_{i:04d}" for i in range(1, 21)]
        
        # User active sessions: {user_id: {"session_id": ..., "content_id": ..., "pos": ..., "played": ..., "device_id": ..., "device_type": ...}}
        self.active_playback_sessions = {}
        
        # Recent event IDs for duplicate injection
        self.emitted_event_ids = []
        
        # Stats
        self.events_emitted = 0
        self.invalid_events_emitted = 0

        self.producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
            linger_ms=10,
            batch_size=32768
        )

    def _get_or_create_session(self, user_id):
        if user_id not in self.active_playback_sessions:
            self.active_playback_sessions[user_id] = {
                "session_id": str(uuid.uuid4()),
                "content_id": random.choice(self.content_pool),
                "device_id": str(uuid.uuid4()),
                "device_type": random.choice(DEVICE_TYPES),
                "app_version": random.choice(APP_VERSIONS),
                "position_seconds": 0.0,
                "playback_seconds": 0.0,
                "state": "stopped"
            }
        return self.active_playback_sessions[user_id]

    def _generate_playback_event(self, now):
        user_id = random.choice(self.user_ids)
        session = self._get_or_create_session(user_id)
        current_state = session["state"]

        # State transition logic
        if current_state == "stopped":
            event_type = "video_play"
            session["state"] = "playing"
            session["position_seconds"] = 0.0
            session["playback_seconds"] = 0.0
        elif current_state == "playing":
            transition = random.choices(["pause", "seek", "complete", "continue"], weights=[0.2, 0.15, 0.05, 0.6])[0]
            if transition == "pause":
                event_type = "video_pause"
                session["state"] = "paused"
                progress = random.uniform(10.0, 180.0)
                session["position_seconds"] += progress
                session["playback_seconds"] += progress
            elif transition == "seek":
                event_type = "video_seek"
                session["position_seconds"] += random.uniform(-60.0, 300.0)
                if session["position_seconds"] < 0:
                    session["position_seconds"] = 0.0
            elif transition == "complete":
                event_type = "video_complete"
                session["position_seconds"] = 7200.0 # completed full runtime
                session["playback_seconds"] += 300.0
                session["state"] = "stopped"
            else:
                event_type = "video_play"
                progress = random.uniform(15.0, 60.0)
                session["position_seconds"] += progress
                session["playback_seconds"] += progress
        else: # paused
            event_type = "video_play"
            session["state"] = "playing"

        ev_id = str(uuid.uuid4())
        event = {
            "event_id": ev_id,
            "event_type": event_type,
            "schema_version": "1.0.0",
            "user_id": user_id,
            "session_id": session["session_id"],
            "device_id": session["device_id"],
            "device_type": session["device_type"],
            "app_version": session["app_version"],
            "content_id": session["content_id"],
            "position_seconds": round(session["position_seconds"], 2),
            "playback_seconds": round(session["playback_seconds"], 2),
            "event_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "ingestion_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        }

        # Handle session completion reset
        if event_type == "video_complete":
            self.active_playback_sessions.pop(user_id, None)

        return TOPIC_PLAYBACK, user_id, event

    def _generate_search_event(self, now):
        user_id = random.choice(self.user_ids)
        query = random.choice(SEARCH_QUERIES)
        has_click = random.random() < 0.65
        selected_content = random.choice(self.content_pool) if has_click else None

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "search",
            "schema_version": "1.0.0",
            "user_id": user_id,
            "session_id": str(uuid.uuid4()),
            "device_id": str(uuid.uuid4()),
            "device_type": random.choice(DEVICE_TYPES),
            "app_version": random.choice(APP_VERSIONS),
            "query_text": query,
            "results_count": random.randint(3, 25),
            "selected_content_id": selected_content,
            "event_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "ingestion_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        }
        return TOPIC_SEARCH, user_id, event

    def _generate_recommendation_event(self, now):
        user_id = random.choice(self.user_ids)
        is_click = random.random() < 0.18 # 18% CTR on rails
        recommended = random.sample(self.content_pool, 5)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "recommendation_click" if is_click else "recommendation_impression",
            "schema_version": "1.0.0",
            "user_id": user_id,
            "session_id": str(uuid.uuid4()),
            "device_id": str(uuid.uuid4()),
            "model_version": "hybrid_collaborative_v2",
            "recommended_content_ids": recommended,
            "clicked_content_id": random.choice(recommended) if is_click else None,
            "position_index": random.randint(0, 4) if is_click else 0,
            "event_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "ingestion_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        }
        return TOPIC_RECOMMENDATION, user_id, event

    def _inject_failure(self, event, now):
        failure_type = random.choice([
            "missing_content_id", "invalid_event_type", "duplicate_event_id",
            "bad_schema_version", "future_timestamp"
        ])

        if failure_type == "missing_content_id" and "content_id" in event:
            event["content_id"] = None
        elif failure_type == "invalid_event_type":
            event["event_type"] = "corrupt_action_unknown"
        elif failure_type == "duplicate_event_id" and self.emitted_event_ids:
            event["event_id"] = random.choice(self.emitted_event_ids)
        elif failure_type == "bad_schema_version":
            event["schema_version"] = "99.99.99-incompatible"
        elif failure_type == "future_timestamp":
            future_time = now + timedelta(days=random.randint(5, 30))
            event["event_time"] = future_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        self.invalid_events_emitted += 1
        return event

    def run(self, total_events=None, duration_seconds=None):
        print(f"Starting event generator [Users: {self.num_users}, Base EPS: {self.base_eps}, Invalid Rate: {self.invalid_rate}]...")
        start_time = time.time()
        last_spike_check = time.time()
        is_spiking = False

        try:
            while True:
                now_utc = datetime.now(timezone.utc)
                current_time = time.time()

                # Traffic Spike simulation
                if current_time - last_spike_check > self.spike_interval:
                    is_spiking = not is_spiking
                    last_spike_check = current_time
                    if is_spiking:
                        print(f" >>> [TRAFFIC SPIKE ACTIVE] Target EPS increased by {self.spike_multiplier}x! <<<")
                    else:
                        print(" >>> [TRAFFIC SPIKE ENDED] Resuming normal throughput. <<<")

                current_target_eps = int(self.base_eps * (self.spike_multiplier if is_spiking else 1.0))
                batch_size = max(1, current_target_eps // 10)
                sleep_interval = 0.1

                for _ in range(batch_size):
                    category = random.choices(["playback", "search", "recs"], weights=[0.70, 0.15, 0.15])[0]
                    if category == "playback":
                        topic, key, ev = self._generate_playback_event(now_utc)
                    elif category == "search":
                        topic, key, ev = self._generate_search_event(now_utc)
                    else:
                        topic, key, ev = self._generate_recommendation_event(now_utc)

                    # Anomaly injection
                    if self.invalid_rate > 0.0 and random.random() < self.invalid_rate:
                        ev = self._inject_failure(ev, now_utc)

                    self.producer.send(topic, key=key, value=ev)
                    self.emitted_event_ids.append(ev.get("event_id"))
                    if len(self.emitted_event_ids) > 1000:
                        self.emitted_event_ids.pop(0)

                    self.events_emitted += 1
                    if total_events and self.events_emitted >= total_events:
                        break

                self.producer.flush()

                # Status log every 500 events
                if self.events_emitted % 500 < batch_size:
                    elapsed = time.time() - start_time
                    actual_eps = self.events_emitted / max(elapsed, 0.001)
                    print(f"Progress: Emitted {self.events_emitted} events ({self.invalid_events_emitted} invalid) | Throughput: {actual_eps:.1f} events/s")

                if total_events and self.events_emitted >= total_events:
                    print(f"Target count of {total_events} events reached.")
                    break

                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    print(f"Duration limit of {duration_seconds}s reached.")
                    break

                time.sleep(sleep_interval)

        except KeyboardInterrupt:
            print("\nGenerator stopped by user.")
        finally:
            self.producer.flush()
            self.producer.close()
            total_time = time.time() - start_time
            print(f"\nFinal Summary: Emitted {self.events_emitted} events in {total_time:.2f}s ({self.events_emitted / max(total_time, 0.001):.1f} avg eps). Invalid count: {self.invalid_events_emitted}.")

def main():
    parser = argparse.ArgumentParser(description="Sokti OTT Event Generator")
    parser.add_argument("--users", type=int, default=1000, help="Number of simulated users")
    parser.add_argument("--events-per-second", type=int, default=100, help="Target events per second")
    parser.add_argument("--total-events", type=int, default=None, help="Stop after N events")
    parser.add_argument("--duration-seconds", type=int, default=None, help="Stop after N seconds")
    parser.add_argument("--invalid-rate", type=float, default=0.0, help="Rate of invalid/corrupt events [0.0 - 1.0]")
    parser.add_argument("--spike-interval", type=int, default=30, help="Seconds between spike cycles")
    parser.add_argument("--spike-multiplier", type=float, default=3.0, help="Spike traffic multiplier")

    args = parser.parse_args()
    generator = EventGenerator(
        num_users=args.users,
        eps=args.events_per_second,
        invalid_rate=args.invalid_rate,
        spike_interval=args.spike_interval,
        spike_multiplier=args.spike_multiplier
    )
    generator.run(total_events=args.total_events, duration_seconds=args.duration_seconds)

if __name__ == "__main__":
    main()
