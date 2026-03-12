# scripts/test_context.py

from __future__ import annotations

import argparse
import asyncio
import textwrap

from app.context_builder import build_rag_context
from app.db import SessionLocal
from app.embedder import EmbeddingClient
from app.retrieval import search_similar_chunks


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--daire-filter", default=None)
    args = parser.parse_args()

    embedder = EmbeddingClient()
    query_embedding = await embedder.embed(args.query)

    db = SessionLocal()
    try:
        results = search_similar_chunks(
            db=db,
            query_embedding=query_embedding,
            final_top_k=args.top_k,
            raw_top_k=30,
            max_chunks_per_case=1,
            daire_filter=args.daire_filter,
        )
    finally:
        db.close()

    context = build_rag_context(results, max_items=args.top_k)

    print("=" * 120)
    print("QUERY")
    print(args.query)
    print("=" * 120)
    print("RAG CONTEXT")
    print(textwrap.shorten(context, width=8000, placeholder="\n...[TRUNCATED]..."))
    print()
    print("=" * 120)
    print(f"CONTEXT LENGTH: {len(context)}")
    print("=" * 120)


if __name__ == "__main__":
    asyncio.run(main())