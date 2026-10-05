"""
Sokti Semantic Chunker
Extracts content metadata from MongoDB and generates rich semantic chunks for embedding.
"""

import os
from typing import List, Dict, Any
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.chunker")

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27018"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
MONGO_PASS = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin")
MONGO_DB = os.getenv("MONGO_DATABASE", "sokti_metadata")


def get_mongo_db():
    client = MongoClient(
        f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
    )
    return client[MONGO_DB]


def extract_and_chunk_catalog() -> List[Dict[str, Any]]:
    """Scan movies and series from MongoDB, generating structured chunks."""
    db = get_mongo_db()
    chunks = []

    # 1. Process Movies
    movies = list(db["movies"].find({}))
    logger.info("Found %d movies in MongoDB for chunking", len(movies))
    for m in movies:
        c_id = m.get("content_id")
        title = m.get("title", "")
        genres = ", ".join(m.get("genres", []))
        synopsis = m.get("synopsis", "")
        director = m.get("director", "")
        cast = ", ".join(m.get("cast", []))
        keywords = ", ".join(m.get("keywords", []))
        language = m.get("language", "en")
        year = m.get("release_year", 2024)
        rating = m.get("maturity_rating", "U/A 13+")

        # Chunk 0: Overview & Narrative
        chunk_0_text = f"Title: {title} ({year}). Genres: {genres}. Language: {language}. Rating: {rating}. Synopsis: {synopsis}"
        chunks.append({
            "content_id": c_id,
            "chunk_index": 0,
            "chunk_type": "synopsis",
            "chunk_text": chunk_0_text,
            "metadata": {
                "title": title,
                "genres": m.get("genres", []),
                "language": language,
                "release_year": year,
                "director": director,
                "type": "movie"
            }
        })

        # Chunk 1: Cast, Crew & Atmospheric Keywords
        chunk_1_text = f"{title}: Directed by {director}. Starring {cast}. Key Themes & Tropes: {keywords}."
        chunks.append({
            "content_id": c_id,
            "chunk_index": 1,
            "chunk_type": "cast_and_themes",
            "chunk_text": chunk_1_text,
            "metadata": {
                "title": title,
                "director": director,
                "cast": m.get("cast", []),
                "keywords": m.get("keywords", []),
                "type": "movie"
            }
        })

    # 2. Process Series
    series = list(db["series"].find({}))
    logger.info("Found %d series in MongoDB for chunking", len(series))
    for s in series:
        c_id = s.get("content_id")
        title = s.get("title", "")
        genres = ", ".join(s.get("genres", []))
        synopsis = s.get("synopsis", "")
        creator = s.get("creator", "")
        cast = ", ".join(s.get("cast", []))
        keywords = ", ".join(s.get("keywords", []))
        language = s.get("language", "en")
        seasons = s.get("seasons_count", 1)

        chunk_text = f"Series: {title}. Seasons: {seasons}. Genres: {genres}. Created by: {creator}. Starring: {cast}. Synopsis: {synopsis}. Tropes: {keywords}."
        chunks.append({
            "content_id": c_id,
            "chunk_index": 0,
            "chunk_type": "series_overview",
            "chunk_text": chunk_text,
            "metadata": {
                "title": title,
                "genres": s.get("genres", []),
                "language": language,
                "creator": creator,
                "type": "series"
            }
        })

    logger.info("Extracted %d total semantic chunks across catalog", len(chunks))
    return chunks


if __name__ == "__main__":
    catalog_chunks = extract_and_chunk_catalog()
    print(f"Sample Chunk 0:\n{catalog_chunks[0]}")
