"""
Sokti Vector Search Engine
Performs approximate nearest neighbor (ANN) cosine similarity search in pgvector.
"""

import json
import logging
import os
import sys
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from fastembed import TextEmbedding

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.vector_search")

PGVECTOR_HOST = os.getenv("PGVECTOR_HOST", "localhost")
PGVECTOR_PORT = int(os.getenv("PGVECTOR_PORT", "15433"))
PGVECTOR_USER = os.getenv("POSTGRES_USER", "postgres")
PGVECTOR_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PGVECTOR_DB = os.getenv("POSTGRES_DB", "postgres")

EMBED_MODEL_NAME = "BAAI/bge-small-en-v1.5"


def get_pgvector_conn():
    return psycopg2.connect(
        host=PGVECTOR_HOST,
        port=PGVECTOR_PORT,
        user=PGVECTOR_USER,
        password=PGVECTOR_PASSWORD,
        dbname=PGVECTOR_DB,
    )


class VectorSearchEngine:
    def __init__(self, model_name: str = EMBED_MODEL_NAME):
        self.model = TextEmbedding(model_name=model_name)

    def search_semantic(
        self,
        query: str,
        limit: int = 5,
        genre_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search content chunks by semantic cosine similarity."""
        # 1. Embed query
        query_vec = list(self.model.embed([query]))[0].tolist()

        # 2. Query pgvector
        conn = get_pgvector_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        sql = """
        SELECT
            content_id,
            chunk_type,
            chunk_text,
            metadata,
            1 - (embedding <=> %(query_vec)s::vector) AS similarity_score
        FROM content_embeddings
        """
        params = {"query_vec": query_vec, "limit": limit}

        if genre_filter:
            sql += " WHERE metadata->'genres' ? %(genre)s "
            params["genre"] = genre_filter

        sql += " ORDER BY embedding <=> %(query_vec)s::vector LIMIT %(limit)s;"

        cur.execute(sql, params)
        results = cur.fetchall()

        cur.close()
        conn.close()

        # Format results
        hits = []
        for r in results:
            hits.append({
                "content_id": r["content_id"],
                "similarity_score": round(float(r["similarity_score"]), 4),
                "chunk_type": r["chunk_type"],
                "chunk_text": r["chunk_text"],
                "metadata": r["metadata"],
            })
        return hits


if __name__ == "__main__":
    searcher = VectorSearchEngine()
    test_query = "cyberpunk sci-fi thriller about artificial intelligence and rogue robots"
    hits = searcher.search_semantic(test_query, limit=3)
    print(f"Query: '{test_query}'")
    for idx, hit in enumerate(hits, 1):
        print(f"Hit {idx} (Score: {hit['similarity_score']}): {hit['content_id']} - {hit['chunk_text']}")
