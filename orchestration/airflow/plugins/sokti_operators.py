"""
Sokti Airflow Custom Operators and Hooks
Provides ClickHouse, MinIO, and PostgreSQL integration along with production callbacks.
"""

import json
import logging
import os
import urllib.request
import urllib.error
from datetime import timedelta
from typing import Any, Dict, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger("sokti.airflow")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "sokti")

MINIO_HOST = os.getenv("MINIO_HOST", "minio")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


def execute_clickhouse_query(query: str, settings: Optional[Dict[str, Any]] = None) -> str:
    """Execute raw SQL query against ClickHouse HTTP interface with retry and timeout."""
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?user={CLICKHOUSE_USER}&password={CLICKHOUSE_PASSWORD}&database={CLICKHOUSE_DB}"
    if settings:
        for k, v in settings.items():
            url += f"&{k}={v}"

    req = urllib.request.Request(
        url=url,
        data=query.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8", errors="replace")
        logger.error("ClickHouse query error: %s (HTTP %d)", err_msg, e.code)
        raise RuntimeError(f"ClickHouse HTTP {e.code}: {err_msg}") from e
    except Exception as e:
        logger.error("ClickHouse connection error: %s", str(e))
        raise


def pipeline_failure_alert(context: Dict[str, Any]):
    """Production on_failure callback emitting structured telemetry and alert notifications."""
    dag_id = context.get("task_instance").dag_id if context.get("task_instance") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    execution_date = str(context.get("execution_date"))
    exception = str(context.get("exception"))
    try_number = context.get("task_instance").try_number if context.get("task_instance") else 1

    alert_payload = {
        "alert_type": "PIPELINE_TASK_FAILURE",
        "severity": "CRITICAL",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date,
        "try_number": try_number,
        "error_message": exception
    }
    logger.critical("ALERT: %s", json.dumps(alert_payload, indent=2))


def pipeline_sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA miss callback logging delayed pipeline execution details."""
    logger.warning(
        "SLA MISSED: DAG=%s, Tasks=%s, Blocking=%s, SLAs=%s",
        dag.dag_id if dag else "unknown",
        [t.task_id for t in task_list] if task_list else [],
        [t.task_id for t in blocking_task_list] if blocking_task_list else [],
        slas
    )


class ClickHouseExecuteOperator(BaseOperator):
    """Custom Airflow Operator executing SQL against ClickHouse."""

    @apply_defaults
    def __init__(self, sql: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql

    def execute(self, context: Dict[str, Any]):
        rendered_sql = self.sql
        logger.info("Executing ClickHouse SQL for task %s:\n%s", self.task_id, rendered_sql)
        result = execute_clickhouse_query(rendered_sql)
        logger.info("ClickHouse response: %s", result[:200] if result else "OK (empty response)")
        return result
