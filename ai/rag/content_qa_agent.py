"""
Sokti Content RAG Agent
Combines pgvector semantic retrieval with augmented generation for OTT discovery.
"""

import json
import logging
import os
import sys
from typing import List, Dict, Any, Optional

# Add repo root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from ai.retrieval.vector_search import VectorSearchEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sokti.rag_agent")


class ContentRAGAgent:
    def __init__(self):
        self.searcher = VectorSearchEngine()

    def answer_query(self, query: str, top_k: int = 3) -> Dict[str, Any]:
        """Perform RAG: Retrieve context from pgvector and generate recommendation response."""
        logger.info("Executing RAG pipeline for user query: '%s'", query)
        
        # Step 1: Retrieval
        hits = self.searcher.search_semantic(query, limit=top_k)
        if not hits:
            return {
                "query": query,
                "answer": "Sorry, I couldn't find any relevant titles in the Sokti catalog matching your query.",
                "sources": [],
            }

        # Step 2: Context Assembly & Synthesis
        context_snippets = []
        sources = []
        for h in hits:
            meta = h.get("metadata", {})
            title = meta.get("title", h["content_id"])
            context_snippets.append(f"- **{title}** ({h['content_id']}): {h['chunk_text']} [Match Score: {h['similarity_score']}]")
            sources.append({
                "content_id": h["content_id"],
                "title": title,
                "score": h["similarity_score"],
                "genres": meta.get("genres", []),
                "type": meta.get("type", "content")
            })

        # Structured RAG Answer
        best_hit = hits[0]
        best_meta = best_hit.get("metadata", {})
        best_title = best_meta.get("title", best_hit["content_id"])
        
        grounded_answer = (
            f"Based on your interest in '{query}', here are top recommendations from Sokti:\n\n"
            f"1. **{best_title}** is your closest match (relevance: {best_hit['similarity_score']*100:.1f}%). "
            f"It features: {best_hit['chunk_text']}\n\n"
            f"Other strong contenders in our catalog include "
            + ", ".join([f"**{s['title']}**" for s in sources[1:]]) + "."
        )

        return {
            "query": query,
            "answer": grounded_answer,
            "sources": sources,
            "retrieval_count": len(sources),
        }


if __name__ == "__main__":
    agent = ContentRAGAgent()
    q = "Show me space exploration and wormholes saving humanity"
    res = agent.answer_query(q)
    print("RAG Response:")
    print(res["answer"])
    print("\nSources:", json.dumps(res["sources"], indent=2))
