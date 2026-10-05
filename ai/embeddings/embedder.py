"""
Sokti Vector Embedder
Encodes catalog chunks into 384-dimensional vectors and stores them in PostgreSQL pgvector.
"""

import json
import logging
import os
import sys
from typing import List, Dict, Any
import numpy as np
import psycopg2
from psycopg2.extras import Json, execute_values
from fastembed import TextEmbedding

# Add repo root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from ai.chunking.chunker import extract_and_chunk_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.embedder")

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


class CatalogEmbedder:
    def __init__(self, model_name: str = EMBED_MODEL_NAME):
        logger.info("Initializing FastEmbed text embedding model: %s", model_name)
        self.model = TextEmbedding(model_name=model_name)

    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate normalized 384-dimensional vector embeddings."""
        embeddings = list(self.model.embed(texts))
        return [e.tolist() for e in embeddings]

    def sync_catalog_embeddings(self):
        """Extract chunks from MongoDB, compute embeddings, and load into pgvector."""
        chunks = extract_and_chunk_catalog()
        if not chunks:
            logger.warning("No catalog chunks found to embed.")
            return

        texts = [c["chunk_text"] for c in chunks]
        logger.info("Computing embeddings for %d catalog chunks...", len(texts))
        vectors = self.generate_embeddings(texts)

        conn = get_pgvector_conn()
        cur = conn.cursor()

        # Clean existing embeddings for fresh load
        cur.execute("TRUNCATE TABLE content_embeddings RESTART IDENTITY;")

        insert_sql = """
        INSERT INTO content_embeddings (content_id, chunk_index, chunk_type, chunk_text, metadata, embedding)
        VALUES %s
        """
        records = []
        for chunk, vec in zip(chunks, vectors):
            records.append((
                chunk["content_id"],
                chunk["chunk_index"],
                chunk["chunk_type"],
                chunk["chunk_text"],
                Json(chunk["metadata"]),
                vec
            ))

        execute_values(cur, insert_sql, records, template="(%s, %s, %s, %s, %s, %s::vector)")
        conn.commit()

        cur.execute("SELECT count(*) FROM content_embeddings;")
        count = cur.fetchone()[0]
        logger.info("Successfully populated %d vector embeddings in pgvector!", count)

        cur.close()
        conn.close()


if __name__ == "__main__":
    embedder = CatalogEmbedder()
    embedder.sync_catalog_embeddings()
