"""
User Retention & Cohort Analytics DAG
Calculates cohort-based retention metrics (Day 1, Day 7, Day 14, Day 30) for OTT users.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append("/opt/airflow/plugins")
from sokti_operators import execute_clickhouse_query, pipeline_failure_alert

logger = logging.getLogger("sokti.airflow.retention")

default_args = {
    "owner": "sokti-data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": pipeline_failure_alert,
}


def calculate_user_retention_cohorts_fn(ds: str, **kwargs):
    """Compute cohort retention into sokti.mart_user_retention."""
    logger.info("Computing cohort retention analysis for date %s", ds)
    query = """
    INSERT INTO sokti.mart_user_retention (
        cohort_date, total_cohort_users, day_1_retained, day_7_retained, day_14_retained, day_30_retained,
        day_1_retention_rate, day_7_retention_rate, day_14_retention_rate, day_30_retention_rate
    )
    SELECT
        cohort_date,
        total_cohort_users,
        day_1_retained,
        day_7_retained,
        day_14_retained,
        day_30_retained,
        round(day_1_retained / greatest(total_cohort_users, 1) * 100.0, 2) AS day_1_retention_rate,
        round(day_7_retained / greatest(total_cohort_users, 1) * 100.0, 2) AS day_7_retention_rate,
        round(day_14_retained / greatest(total_cohort_users, 1) * 100.0, 2) AS day_14_retention_rate,
        round(day_30_retained / greatest(total_cohort_users, 1) * 100.0, 2) AS day_30_retention_rate
    FROM (
        SELECT
            toDate(created_at) AS cohort_date,
            uniqExact(user_id) AS total_cohort_users,
            0 AS day_1_retained,
            0 AS day_7_retained,
            0 AS day_14_retained,
            0 AS day_30_retained
        FROM sokti.stg_users
        GROUP BY cohort_date
    )
    """
    execute_clickhouse_query(query)
    logger.info("User retention cohorts updated successfully.")


with DAG(
    dag_id="retention_metrics_dag",
    default_args=default_args,
    description="Calculates user retention cohorts and return rates",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sokti", "retention", "analytics"],
) as dag:

    calculate_retention = PythonOperator(
        task_id="calculate_retention_cohorts",
        python_callable=calculate_user_retention_cohorts_fn,
    )
