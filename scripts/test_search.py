# scripts/test_search.py

from __future__ import annotations

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import argparse
import asyncio
import textwrap

from app.db import SessionLocal
from app.embedder import EmbeddingClient
from app.retrieval import search_similar_chunks


def print_result(item: dict, rank: int) -> None:
    preview = (item["text"] or "").strip().replace("\n", " ")
    preview = textwrap.shorten(preview, width=700, placeholder=" ...")

    print("=" * 120)
    print(f"#{rank}")
    print(f"chunk_id    : {item['id']}")
    print(f"case_id     : {item['case_id']}")
    print(f"chunk_index : {item['chunk_index']}")
    print(f"chunk_type  : {item['chunk_type']}")
    print(f"similarity  : {item['similarity']:.6f}")
    print(f"daire       : {item.get('daire_text')}")
    print(f"esas_no     : {item.get('esas_no')}")
    print(f"karar_no    : {item.get('karar_no')}")
    print(f"tarih       : {item.get('tarih')}")
    print("-" * 120)
    print(preview)
    print()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query",
        required=True,
        help="Aranacak kullanıcı sorusu",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Kaç sonuç dönecek",
    )
    parser.add_argument(
    "--daire-filter",
    default=None,
    help="Daire text için opsiyonel ILIKE filtresi",
)
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

    if not results:
        print("Sonuç bulunamadı.")
        return

    print("\nQUERY:")
    print(args.query)
    print(f"\nTOP_K: {args.top_k}\n")

    for i, item in enumerate(results, start=1):
        print_result(item, i)


if __name__ == "__main__":
    asyncio.run(main())