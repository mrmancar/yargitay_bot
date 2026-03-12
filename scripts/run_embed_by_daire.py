from __future__ import annotations

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import asyncio
from dataclasses import dataclass

from sqlalchemy import text

from app.db import SessionLocal
from app.embedder import EmbeddingClient


BATCH_SIZE = 50
WORKER_COUNT = 4
TARGET_DAIRE = "2. Hukuk Dairesi"


@dataclass
class ChunkRow:
    id: int
    text: str


async def fetch_embedding(embedder: EmbeddingClient, text_value: str) -> list[float]:
    return await embedder.embed(text_value)


def load_target_chunks(limit: int | None = None) -> list[ChunkRow]:
    db = SessionLocal()
    try:
        sql = """
            SELECT id, text
            FROM case_chunks
            WHERE embedding IS NULL
              AND daire_text = :daire_text
              AND text IS NOT NULL
              AND length(trim(text)) > 0
            ORDER BY id
        """

        params = {"daire_text": TARGET_DAIRE}

        if limit is not None:
            sql += " LIMIT :limit"
            params["limit"] = limit

        rows = db.execute(text(sql), params).mappings().all()
        return [ChunkRow(id=row["id"], text=row["text"]) for row in rows]
    finally:
        db.close()


def update_embeddings(results: list[tuple[int, list[float]]]) -> None:
    if not results:
        return

    db = SessionLocal()
    try:
        update_sql = text("""
            UPDATE case_chunks
            SET embedding = CAST(:embedding AS vector)
            WHERE id = :id
        """)

        for chunk_id, embedding in results:
            embedding_str = "[" + ",".join(str(x) for x in embedding) + "]"
            db.execute(
                update_sql,
                {
                    "id": chunk_id,
                    "embedding": embedding_str,
                },
            )

        db.commit()
    finally:
        db.close()


async def worker(
    name: str,
    queue: asyncio.Queue[ChunkRow],
    embedder: EmbeddingClient,
    results: list[tuple[int, list[float]]],
) -> None:
    while True:
        try:
            item = await queue.get()
        except asyncio.CancelledError:
            break

        try:
            emb = await fetch_embedding(embedder, item.text)
            results.append((item.id, emb))
            print(f"[{name}] embedded chunk_id={item.id}")
        except Exception as exc:
            print(f"[{name}] ERROR chunk_id={item.id} -> {exc}")
        finally:
            queue.task_done()


async def process_chunks(limit: int | None = None) -> None:
    chunks = load_target_chunks(limit=limit)
    total = len(chunks)

    print(f"Target daire: {TARGET_DAIRE}")
    print(f"Toplam embedlenecek chunk: {total}")

    if total == 0:
        print("Embedlenecek kayıt yok.")
        return

    embedder = EmbeddingClient()

    for start in range(0, total, BATCH_SIZE):
        batch = chunks[start:start + BATCH_SIZE]

        queue: asyncio.Queue[ChunkRow] = asyncio.Queue()
        for item in batch:
            await queue.put(item)

        batch_results: list[tuple[int, list[float]]] = []

        workers = [
            asyncio.create_task(worker(f"worker-{i+1}", queue, embedder, batch_results))
            for i in range(WORKER_COUNT)
        ]

        await queue.join()

        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

        update_embeddings(batch_results)
        print(f"Committed batch: {start + len(batch)}/{total}")


if __name__ == "__main__":
    asyncio.run(process_chunks())