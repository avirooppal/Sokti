"""
Sokti Multi-Stage Hybrid Recommendation Engine
Combines:
1. Collaborative Filtering (Latent SVD user preferences)
2. Market Basket Analysis (Association Rules / Co-watch Lift)
3. pgvector Semantic Search (HNSW Cosine embedding similarity)
Ranks and dedupes candidate content for production OTT slates.
"""

import logging
import os
import sys
from typing import Dict, List, Optional, Any
import clickhouse_connect
import psycopg2
from psycopg2.extras import RealDictCursor

from ai.recommendations.market_basket import MarketBasketRecommender
from ai.recommendations.matrix_factorization import CollaborativeFilteringEngine
from ai.retrieval.vector_search import VectorSearchEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.hybrid_rec")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASSWORD", "sokti_pass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "sokti")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sokti")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sokti_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sokti_pass")


class HybridRecommendationEngine:
    def __init__(self):
        self.market_basket = MarketBasketRecommender()
        self.cf_engine = CollaborativeFilteringEngine()
        self.ch_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            database=CLICKHOUSE_DB,
        )

    def _get_pg_conn(self):
        return psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

    def get_content_metadata(self, content_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch rich metadata for content IDs from ClickHouse / dim_content."""
        if not content_ids:
            return {}
        formatted_ids = ", ".join([f"'{c}'" for c in content_ids])
        query = f"""
        SELECT 
            content_id,
            title,
            genres,
            release_year,
            duration_seconds,
            maturity_rating
        FROM sokti.dim_content
        WHERE content_id IN ({formatted_ids})
        """
        rows = self.ch_client.query(query).result_rows
        metadata = {}
        for r in rows:
            metadata[r[0]] = {
                "content_id": r[0],
                "title": r[1],
                "genres": r[2],
                "release_year": int(r[3]),
                "duration_seconds": int(r[4]),
                "maturity_rating": r[5],
            }
        return metadata

    def get_user_recent_watches(self, user_id: str, limit: int = 3) -> List[str]:
        """Fetch the most recently watched content IDs for a user."""
        query = f"""
        SELECT content_id
        FROM sokti.raw_playback_events
        WHERE toString(user_id) = '{user_id}' AND content_id != ''
        ORDER BY event_time DESC
        LIMIT {limit}
        """
        rows = self.ch_client.query(query).result_rows
        return [r[0] for r in rows]

    def get_item_to_item_recommendations(self, content_id: str, limit: int = 8) -> List[Dict[str, Any]]:
        """
        Blends Market Basket Co-watch Lift with pgvector HNSW dense semantic similarity.
        Weights:
        - Co-watch Lift / Association: 0.6
        - Semantic Embeddings Similarity: 0.4
        """
        # 1. Market Basket candidates
        mb_candidates = self.market_basket.get_co_watched_recommendations(content_id, limit=limit * 2)
        
        # 2. Semantic Search candidates
        meta = self.get_content_metadata([content_id])
        target_info = meta.get(content_id, {})
        query_text = f"{target_info.get('title', '')} {' '.join(target_info.get('genres', []))}"
        
        semantic_candidates = []
        try:
            vector_engine = VectorSearchEngine()
            semantic_candidates = vector_engine.search_semantic(query=query_text or "drama movie", limit=limit * 2)
        except Exception as e:
            logger.warning("Semantic search fallback: %s", e)

        # Merge scores
        scored_items: Dict[str, Dict[str, Any]] = {}

        # Normalize Lift scores (max lift ~ 10.0)
        for cand in mb_candidates:
            cid = cand["content_id"]
            if cid == content_id:
                continue
            norm_lift = min(cand.get("lift", 1.0) / 10.0, 1.0)
            scored_items[cid] = {
                "content_id": cid,
                "co_watch_score": norm_lift,
                "semantic_score": 0.0,
                "lift": cand.get("lift", 1.0),
                "confidence": cand.get("confidence", 0.0),
                "strategy": "MARKET_BASKET_LIFT",
            }

        # Normalize Semantic scores
        for cand in semantic_candidates:
            cid = cand.get("content_id")
            if not cid or cid == content_id:
                continue
            sim = float(cand.get("similarity_score", 0.5))
            if cid in scored_items:
                scored_items[cid]["semantic_score"] = sim
                scored_items[cid]["strategy"] = "HYBRID_CO_WATCH_SEMANTIC"
            else:
                scored_items[cid] = {
                    "content_id": cid,
                    "co_watch_score": 0.0,
                    "semantic_score": sim,
                    "lift": 1.0,
                    "confidence": 0.0,
                    "strategy": "PGVECTOR_SEMANTIC",
                }

        # Compute blended score
        results = []
        for cid, item in scored_items.items():
            blended = 0.6 * item["co_watch_score"] + 0.4 * item["semantic_score"]
            item["score"] = round(blended, 4)
            results.append(item)

        results.sort(key=lambda x: x["score"], reverse=True)
        top_results = results[:limit]

        # Enrich with metadata
        meta_dict = self.get_content_metadata([r["content_id"] for r in top_results])
        for r in top_results:
            info = meta_dict.get(r["content_id"], {})
            r.update(info)

        return top_results

    def get_personalized_recommendations(self, user_id: str, limit: int = 10) -> Dict[str, Any]:
        """
        Industry-standard personalized home shelf generation:
        1. "Because You Watched [Title]" (Market Basket Co-watch Lift based on user's last watch)
        2. "Top Collaborative Picks" (Netflix Prize SVD Matrix Factorization)
        3. "Trending & High Affinity" (Blended hybrid ensemble)
        """
        # 1. Fetch CF SVD scores
        cf_recs = self.cf_engine.get_collaborative_recommendations(user_id, limit=limit)
        
        # 2. Fetch User recent watch for Market Basket
        recent_watches = self.get_user_recent_watches(user_id, limit=2)
        co_watch_recs = []
        antecedent_title = "Recent Watch"
        if recent_watches:
            last_watched = recent_watches[0]
            co_watch_recs = self.get_item_to_item_recommendations(last_watched, limit=limit)
            last_meta = self.get_content_metadata([last_watched])
            if last_watched in last_meta:
                antecedent_title = last_meta[last_watched]["title"]
        else:
            # Fallback to popular item cnt_mov_0001
            co_watch_recs = self.get_item_to_item_recommendations("cnt_mov_0001", limit=limit)
            antecedent_title = "Inception Protocol"

        # Enrich CF Recs
        cf_cids = [r["content_id"] for r in cf_recs]
        cf_meta = self.get_content_metadata(cf_cids)
        for r in cf_recs:
            r.update(cf_meta.get(r["content_id"], {}))

        # 3. Hybrid Blended Slate
        candidate_pool = {}
        for r in cf_recs:
            cid = r["content_id"]
            candidate_pool[cid] = {
                "content_id": cid,
                "score": round(float(r.get("score", 0.0)) * 0.5, 4),
                "strategy": "COLLABORATIVE_SVD",
                "title": r.get("title", ""),
                "genres": r.get("genres", []),
                "release_year": r.get("release_year", 2024),
                "duration_seconds": r.get("duration_seconds", 5400),
                "maturity_rating": r.get("maturity_rating", "PG-13"),
            }

        for r in co_watch_recs:
            cid = r["content_id"]
            if cid in candidate_pool:
                candidate_pool[cid]["score"] += round(float(r.get("score", 0.0)) * 0.5, 4)
                candidate_pool[cid]["strategy"] = "HYBRID_ENSEMBLE"
            else:
                candidate_pool[cid] = {
                    "content_id": cid,
                    "score": round(float(r.get("score", 0.0)) * 0.5, 4),
                    "strategy": "CO_WATCH_ASSOCIATION",
                    "title": r.get("title", ""),
                    "genres": r.get("genres", []),
                    "release_year": r.get("release_year", 2024),
                    "duration_seconds": r.get("duration_seconds", 5400),
                    "maturity_rating": r.get("maturity_rating", "PG-13"),
                }

        blended_picks = sorted(candidate_pool.values(), key=lambda x: x["score"], reverse=True)[:limit]

        return {
            "user_id": user_id,
            "shelves": {
                "because_you_watched": {
                    "shelf_title": f"Because You Watched {antecedent_title}",
                    "algorithm": "Market Basket Association Rule Mining (Lift > 1.0)",
                    "items": co_watch_recs,
                },
                "collaborative_picks": {
                    "shelf_title": "Top Picks for You",
                    "algorithm": "Latent Matrix Factorization (TruncatedSVD Implicit Feedback)",
                    "items": cf_recs,
                },
                "hybrid_discoveries": {
                    "shelf_title": "AI Hybrid Discoveries",
                    "algorithm": "Multi-Stage Hybrid Ensemble (SVD + Co-watch Lift + pgvector)",
                    "items": blended_picks,
                }
            }
        }


if __name__ == "__main__":
    engine = HybridRecommendationEngine()
    print("Testing Item-to-Item Co-watch + Semantic blending for cnt_mov_0001:")
    item_recs = engine.get_item_to_item_recommendations("cnt_mov_0001", limit=4)
    for r in item_recs:
        print(f"  -> {r.get('title')} ({r.get('content_id')}) | Score: {r.get('score')} | Strategy: {r.get('strategy')}")

    print("\nTesting Personalized User Shelves:")
    user_shelf = engine.get_personalized_recommendations("60587b6a-de1a-5fcf-8069-f1aab9b0db73", limit=4)
    for shelf_key, shelf_data in user_shelf["shelves"].items():
        print(f"Shelf [{shelf_data['shelf_title']}] ({shelf_data['algorithm']}): {len(shelf_data['items'])} items")
