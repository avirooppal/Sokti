import os
import sys
import pytest
from fastapi.testclient import TestClient

from apps.api.main import app as core_app

sys.path.insert(0, os.path.abspath("apps/semantic-search-api"))
import main as search_module
search_app = search_module.app

core_client = TestClient(core_app)
search_client = TestClient(search_app)


def test_core_api_health():
    res = core_client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_search_api_health():
    res = search_client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_semantic_search_returns_hits():
    res = search_client.post(
        "/search/semantic",
        json={"query": "sci-fi space wormhole exploration", "limit": 3}
    )
    assert res.status_code == 200
    data = res.json()
    assert data["results_count"] > 0
    assert len(data["hits"]) <= 3
    assert "similarity_score" in data["hits"][0]


def test_rag_query_generation():
    res = search_client.post(
        "/rag/ask",
        json={"query": "Recommend me an intense crime documentary or thriller", "top_k": 2}
    )
    assert res.status_code == 200
    data = res.json()
    assert "answer" in data
    assert len(data["sources"]) > 0
    assert "content_id" in data["sources"][0]


def test_frontend_serving():
    res = core_client.get("/")
    assert res.status_code == 200
    assert "SOKTI" in res.text


def test_catalog_endpoint():
    res = core_client.get("/api/v1/catalog?limit=5")
    assert res.status_code == 200
    data = res.json()
    assert "items" in data
    assert len(data["items"]) > 0


def test_users_endpoint_pii_masked():
    res = core_client.get("/api/v1/users?limit=3")
    assert res.status_code == 200
    users = res.json()["users"]
    assert len(users) > 0
    # Verify email has masking (contains ***)
    assert "***" in users[0]["email"]

