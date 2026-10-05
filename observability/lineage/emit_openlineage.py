"""
Sokti OpenLineage Event Emitter
Emits OpenLineage 2.0 compliant lineage events describing data flows from Ingestion to Marts.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.openlineage")


def create_openlineage_event(
    job_name: str,
    inputs: List[Dict[str, str]],
    outputs: List[Dict[str, str]],
    event_type: str = "COMPLETE"
) -> Dict[str, Any]:
    """Build OpenLineage standard specification event."""
    now_str = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())

    def format_dataset(ds: Dict[str, str]):
        return {
            "namespace": ds.get("namespace", "sokti"),
            "name": ds["name"],
            "facets": {
                "dataSource": {
                    "name": ds.get("namespace", "sokti"),
                    "uri": f"{ds.get('namespace', 'sokti')}://{ds['name']}"
                }
            }
        }

    event = {
        "eventType": event_type,
        "eventTime": now_str,
        "run": {
            "runId": run_id,
            "facets": {
                "nominalTime": {
                    "nominalStartTime": now_str
                }
            }
        },
        "job": {
            "namespace": "sokti.data.platform",
            "name": job_name,
            "facets": {
                "jobType": {"processingType": "STREAMING" if "streaming" in job_name else "BATCH"}
            }
        },
        "inputs": [format_dataset(i) for i in inputs],
        "outputs": [format_dataset(o) for o in outputs],
        "producer": "https://github.com/sokti/platform/openlineage-emitter:v1.0"
    }
    return event


def export_platform_lineage(output_file: str = "observability/lineage/platform_lineage.json"):
    """Generate and save end-to-end platform lineage graph."""
    stages = [
        {
            "job_name": "cdc_postgres_to_kafka",
            "inputs": [{"namespace": "postgres", "name": "public.users"}, {"namespace": "postgres", "name": "public.subscriptions"}],
            "outputs": [{"namespace": "kafka", "name": "sokti_cdc.public.users"}, {"namespace": "kafka", "name": "sokti_cdc.public.subscriptions"}]
        },
        {
            "job_name": "streaming_playback_enrichment",
            "inputs": [{"namespace": "kafka", "name": "ott.playback.events.v1"}],
            "outputs": [
                {"namespace": "clickhouse", "name": "sokti.raw_playback_events"},
                {"namespace": "clickhouse", "name": "sokti.agg_content_hourly"},
                {"namespace": "kafka", "name": "ott.playback.dlq.v1"}
            ]
        },
        {
            "job_name": "lake_archival_to_minio",
            "inputs": [{"namespace": "kafka", "name": "ott.playback.events.v1"}],
            "outputs": [{"namespace": "minio_s3", "name": "sokti-raw/date=YYYY-MM-DD/hour=HH/"}]
        },
        {
            "job_name": "ai_content_vectorization",
            "inputs": [{"namespace": "mongodb", "name": "sokti_metadata.movies"}, {"namespace": "mongodb", "name": "sokti_metadata.series"}],
            "outputs": [{"namespace": "pgvector", "name": "public.content_embeddings"}]
        },
        {
            "job_name": "dbt_analytics_marts",
            "inputs": [{"namespace": "clickhouse", "name": "sokti.raw_playback_events"}],
            "outputs": [
                {"namespace": "clickhouse", "name": "sokti.mart_daily_platform_metrics"},
                {"namespace": "clickhouse", "name": "sokti.mart_churn_features"},
                {"namespace": "clickhouse", "name": "sokti.mart_user_retention"}
            ]
        }
    ]

    events = [create_openlineage_event(s["job_name"], s["inputs"], s["outputs"]) for s in stages]
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)
    logger.info("Exported %d OpenLineage pipeline events to %s", len(events), output_file)


if __name__ == "__main__":
    export_platform_lineage()
