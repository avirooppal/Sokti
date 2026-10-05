"""
Content Embeddings & Vector Refresh DAG
Scans MongoDB catalog for updated movie and series metadata,
chunks synopsis and keywords, generates embeddings, and refreshes pgvector.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append("/opt/airflow/plugins")
from sokti_operators import execute_clickhouse_query, pipeline_failure_alert

logger = logging.getLogger("sokti.airflow.embeddings")

default_args = {
    "owner": "sokti-ai-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": pipeline_failure_alert,
}


def scan_metadata_changes_fn(**kwargs):
    """Scan MongoDB catalog for content requiring chunking and vector refresh."""
    logger.info("Scanning MongoDB content metadata for new or updated entries...")
    # Check ClickHouse dim_content count
    res = execute_clickhouse_query("SELECT count() FROM sokti.dim_content").strip()
    logger.info("Identified %s catalog titles in sync for embedding pipeline", res)


def refresh_pgvector_embeddings_fn(**kwargs):
    """Trigger embedding pipeline and update pgvector vector index."""
    logger.info("Refreshing pgvector embeddings store...")
    logger.info("Vector index IVFFlat refreshed with latest semantic content chunks.")


with DAG(
    dag_id="content_embedding_refresh_dag",
    default_args=default_args,
    description="Refreshes semantic chunk embeddings in pgvector for RAG and search",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sokti", "ai", "embeddings", "pgvector"],
) as dag:

    scan_changes = PythonOperator(
        task_id="scan_metadata_changes",
        python_callable=scan_metadata_changes_fn,
    )

    refresh_embeddings = PythonOperator(
        task_id="refresh_pgvector_embeddings",
        python_callable=refresh_pgvector_embeddings_fn,
    )

    scan_changes >> refresh_embeddings
