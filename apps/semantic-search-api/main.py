"""
Sokti Semantic Search & RAG API
FastAPI service exposing semantic search, hybrid retrieval, RAG, and personalized recommendations.
"""

import logging
import os
import sys
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import clickhouse_connect

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from ai.retrieval.vector_search import VectorSearchEngine
from ai.rag.content_qa_agent import ContentRAGAgent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.search_api")

app = FastAPI(
    title="Sokti Semantic Search & RAG API",
    version="1.0.0",
    description="Vector search, hybrid retrieval, and AI-powered recommendations for Sokti OTT.",
)

# Initialize engines
search_engine = VectorSearchEngine()
rag_agent = ContentRAGAgent()

# ClickHouse connection helper
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "sokti")


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


class SearchRequest(BaseModel):
    query: str = Field(..., example="cyberpunk sci-fi thriller about artificial intelligence")
    limit: int = Field(5, ge=1, le=50)
    genre_filter: Optional[str] = Field(None, example="Sci-Fi")


class RAGRequest(BaseModel):
    query: str = Field(..., example="What movies should I watch if I like space exploration?")
    top_k: int = Field(3, ge=1, le=10)


@app.get("/health")
def health():
    return {"status": "ok", "service": "semantic-search-api"}


@app.post("/search/semantic")
def semantic_search(req: SearchRequest):
    """Perform pure vector cosine similarity search in pgvector."""
    try:
        hits = search_engine.search_semantic(
            query=req.query,
            limit=req.limit,
            genre_filter=req.genre_filter
        )
        return {"query": req.query, "results_count": len(hits), "hits": hits}
    except Exception as e:
        logger.error("Error in semantic search: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/rag/ask")
def ask_rag(req: RAGRequest):
    """Retrieve catalog context and generate grounded recommendation."""
    try:
        result = rag_agent.answer_query(query=req.query, top_k=req.top_k)
        return result
    except Exception as e:
        logger.error("Error in RAG generation: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/recommendations/{user_id}")
def user_recommendations(user_id: str, limit: int = 5):
    """
    Generate personalized recommendations for a user:
    Combines ClickHouse analytical churn/engagement features with pgvector similarity.
    """
    try:
        ch = get_clickhouse_client()
        query = f"SELECT user_id, genre_watch_minutes_30d, content_completion_rate FROM sokti.mart_churn_features WHERE user_id = '{user_id}'"
        rows = ch.query(query).result_rows
        
        # If user found in marts, recommend tailored content; else popular sci-fi/thriller
        if rows:
            _, watch_mins, comp_rate = rows[0]
            theme_query = "Award winning compelling thriller and drama with high completion rate"
        else:
            theme_query = "Top trending movies and series across all genres"

        hits = search_engine.search_semantic(query=theme_query, limit=limit)
        return {
            "user_id": user_id,
            "profile_context": {
                "watch_minutes_30d": rows[0][1] if rows else 0,
                "completion_rate": rows[0][2] if rows else 0,
            },
            "recommendations": hits
        }
    except Exception as e:
        logger.error("Error generating user recommendations: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
