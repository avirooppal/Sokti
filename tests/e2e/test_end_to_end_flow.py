"""
End-to-end integration tests for Sokti platform.
Validates the data journey from ingestion to analytics and vector search.
"""

import pytest
import psycopg2
import clickhouse_connect
from ai.retrieval.vector_search import VectorSearchEngine
from ai.rag.content_qa_agent import ContentRAGAgent
from data_quality.checks.quality_engine import DataQualityEngine


def test_clickhouse_marts_populated():
    client = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="sokti_pass", database="sokti")
    res = client.query("SELECT count() FROM sokti.mart_churn_features")
    count = res.result_rows[0][0]
    assert count > 0, "mart_churn_features should contain calculated user features"


def test_pgvector_similarity_search():
    engine = VectorSearchEngine()
    hits = engine.search_semantic("epic journey across deep space", limit=2)
    assert len(hits) == 2
    assert hits[0]["similarity_score"] > 0.4


def test_rag_synthesis():
    agent = ContentRAGAgent()
    answer = agent.answer_query("cyberpunk detective and drones")
    assert "answer" in answer
    assert len(answer["sources"]) > 0


def test_data_quality_engine_execution():
    dq = DataQualityEngine()
    freshness = dq.check_freshness()
    assert freshness["status"] in ("PASS", "WARN")
