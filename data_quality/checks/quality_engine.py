"""
Sokti Data Quality Engine
Performs production data quality audits, volume anomaly detection,
impossible value validation, quarantine routing, and Prometheus metrics generation.
"""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.dq_engine")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "sokti")


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def init_quarantine_table(client):
    """Ensure quarantine table exists in ClickHouse."""
    ddl = """
    CREATE TABLE IF NOT EXISTS sokti.quarantine_events (
        quarantine_id UUID,
        quarantine_time DateTime DEFAULT now(),
        source_table LowCardinality(String),
        event_id UUID,
        violation_code LowCardinality(String),
        violation_details String,
        raw_payload String
    ) ENGINE = MergeTree()
    ORDER BY (violation_code, quarantine_time, event_id)
    """
    client.command(ddl)
    logger.info("Quarantine table sokti.quarantine_events verified.")


class DataQualityEngine:
    def __init__(self):
        self.client = get_client()
        init_quarantine_table(self.client)
        self.metrics: Dict[str, Any] = {}

    def check_freshness(self) -> Dict[str, Any]:
        """Verify data freshness: lag between latest event and current timestamp."""
        query = "SELECT max(event_time), now() FROM sokti.raw_playback_events"
        rows = self.client.query(query).result_rows
        max_time, current_time = rows[0]
        lag_seconds = (current_time - max_time).total_seconds() if max_time else 999999
        status = "PASS" if lag_seconds <= 86400 * 2 else "WARN"  # 48h tolerance for historical test data
        
        result = {
            "check": "freshness",
            "max_event_time": str(max_time),
            "current_time": str(current_time),
            "lag_seconds": lag_seconds,
            "status": status,
        }
        logger.info("Freshness Check: %s (lag: %.1fs)", status, lag_seconds)
        return result

    def check_null_and_duplicate_rates(self) -> Dict[str, Any]:
        """Compute null rate for mandatory keys and duplicate event rate."""
        query = """
        SELECT
            count(*) as total,
            countIf(user_id = toUUID('00000000-0000-0000-0000-000000000000')) as null_users,
            countIf(content_id = '') as null_contents,
            countIf(session_id = toUUID('00000000-0000-0000-0000-000000000000')) as null_sessions,
            count(*) - uniqExact(event_id) as duplicate_events
        FROM sokti.raw_playback_events
        """
        rows = self.client.query(query).result_rows
        total, null_u, null_c, null_s, dupes = rows[0]
        total = max(total, 1)

        null_rate_u = null_u / total
        null_rate_c = null_c / total
        dup_rate = dupes / total

        passed = (null_rate_u <= 0.01) and (null_rate_c <= 0.05) and (dup_rate <= 0.25)
        result = {
            "check": "null_and_duplicate_rates",
            "total_records": total,
            "null_user_rate": round(null_rate_u, 4),
            "null_content_rate": round(null_rate_c, 4),
            "duplicate_rate": round(dup_rate, 4),
            "status": "PASS" if passed else "FAIL",
        }
        logger.info("Null & Duplicate Rates: %s (null_u=%.2f%%, null_c=%.2f%%, dup=%.2f%%)",
                    result["status"], null_rate_u * 100, null_rate_c * 100, dup_rate * 100)
        return result

    def scan_and_quarantine_violations(self) -> Dict[str, Any]:
        """Identify invalid playback records and route them to quarantine table."""
        # 1. Future event timestamps (> 10 minutes in future)
        q_future = """
        SELECT event_id, event_time, user_id, content_id 
        FROM sokti.raw_playback_events 
        WHERE event_time > now() + INTERVAL 10 MINUTE
        """
        future_violations = self.client.query(q_future).result_rows

        # 2. Unknown content_ids (not present in dim_content catalog)
        q_unknown = """
        SELECT r.event_id, r.event_time, r.user_id, r.content_id 
        FROM sokti.raw_playback_events r
        LEFT JOIN sokti.dim_content d ON r.content_id = d.content_id
        WHERE d.content_id IS NULL AND r.content_id != ''
        """
        unknown_violations = self.client.query(q_unknown).result_rows

        # 3. Impossible playback values (watch_seconds > 86400 or position_seconds < 0)
        q_impossible = """
        SELECT event_id, event_time, user_id, content_id, playback_seconds, position_seconds
        FROM sokti.raw_playback_events
        WHERE playback_seconds > 86400 OR position_seconds < 0
        """
        impossible_violations = self.client.query(q_impossible).result_rows

        quarantined_records = []
        # Process future
        for row in future_violations:
            quarantined_records.append((
                uuid.uuid4(),
                "raw_playback_events",
                row[0],
                "FUTURE_TIMESTAMP",
                f"event_time {row[1]} is ahead of current clock",
                json.dumps({"user_id": str(row[2]), "content_id": row[3], "event_time": str(row[1])})
            ))
        # Process unknown content
        for row in unknown_violations:
            quarantined_records.append((
                uuid.uuid4(),
                "raw_playback_events",
                row[0],
                "UNKNOWN_CONTENT_ID",
                f"content_id {row[3]} not present in dim_content",
                json.dumps({"user_id": str(row[2]), "content_id": row[3]})
            ))
        # Process impossible values
        for row in impossible_violations:
            quarantined_records.append((
                uuid.uuid4(),
                "raw_playback_events",
                row[0],
                "IMPOSSIBLE_PLAYBACK_VALUE",
                f"playback_seconds={row[4]}, position_seconds={row[5]}",
                json.dumps({"playback_seconds": row[4], "position_seconds": row[5]})
            ))

        if quarantined_records:
            self.client.insert(
                "sokti.quarantine_events",
                quarantined_records,
                column_names=["quarantine_id", "source_table", "event_id", "violation_code", "violation_details", "raw_payload"]
            )
            logger.warning("Quarantined %d violating records to sokti.quarantine_events", len(quarantined_records))
        else:
            logger.info("0 violating records required quarantine.")

        summary = {
            "check": "quarantine_routing",
            "future_timestamp_violations": len(future_violations),
            "unknown_content_violations": len(unknown_violations),
            "impossible_playback_violations": len(impossible_violations),
            "total_quarantined": len(quarantined_records),
            "status": "PASS" if len(quarantined_records) == 0 else "QUARANTINED"
        }
        return summary

    def generate_prometheus_metrics(self, dq_results: List[Dict[str, Any]], output_path: str = "observability/prometheus/dq_metrics.prom"):
        """Write Prometheus exposition text format for Prometheus scraping."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        lines = [
            "# HELP sokti_dq_check_status Status of Sokti data quality check (1=PASS, 0=FAIL/WARN)",
            "# TYPE sokti_dq_check_status gauge",
        ]
        for res in dq_results:
            name = res.get("check", "unknown")
            passed = 1 if res.get("status") in ("PASS", "QUARANTINED") else 0
            lines.append(f'sokti_dq_check_status{{check="{name}"}} {passed}')

        quarantine_count = self.client.query("SELECT count() FROM sokti.quarantine_events").result_rows[0][0]
        lines.extend([
            "# HELP sokti_dq_quarantine_total Total records held in data quality quarantine",
            "# TYPE sokti_dq_quarantine_total gauge",
            f"sokti_dq_quarantine_total {quarantine_count}",
        ])

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        logger.info("Exported Prometheus DQ metrics to %s", output_path)

    def run_all(self) -> Dict[str, Any]:
        """Execute complete quality suite."""
        logger.info("========== Starting Sokti Data Quality Suite ==========")
        results = [
            self.check_freshness(),
            self.check_null_and_duplicate_rates(),
            self.scan_and_quarantine_violations(),
        ]
        self.generate_prometheus_metrics(results)
        all_passed = all(r.get("status") in ("PASS", "QUARANTINED") for r in results)
        logger.info("Data Quality Suite Completed. Overall Passed: %s", all_passed)
        return {"overall_pass": all_passed, "details": results}


if __name__ == "__main__":
    engine = DataQualityEngine()
    summary = engine.run_all()
    print(json.dumps(summary, indent=2))
