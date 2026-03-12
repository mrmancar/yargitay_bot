import asyncio
import os
import sys
import time
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import ollama

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MODEL_NAME = "embeddinggemma"

BATCH_SIZE = 50
WORKER_COUNT = 4

DATABASE_URL = "postgresql+asyncpg://yargitay_user:123456@localhost/yargitay_bot"

engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)


async def update_embeddings(batch, embeddings):

    async with AsyncSessionLocal() as session:

        for row, embedding in zip(batch, embeddings):

            await session.execute(
                text("""
                UPDATE case_chunks
                SET embedding = CAST(:embedding AS vector)
                WHERE id = :id
                """),
                {
                    "id": row.id,
                    "embedding": str(embedding)
                }
            )

        await session.commit()


async def worker(queue, client, stats):

    while True:

        batch = await queue.get()

        if batch is None:
            queue.task_done()
            return

        start = time.time()

        try:

            texts = [r.text or "" for r in batch]

            response = await client.embed(
                model=MODEL_NAME,
                input=texts
            )

            await update_embeddings(batch, response.embeddings)

            processed = len(batch)

            stats["done"] += processed

        except Exception as e:

            print("Worker error:", e)
            processed = 0

        elapsed = time.time() - start

        speed = stats["done"] / (time.time() - stats["start"])

        remaining = stats["total"] - stats["done"]

        eta = remaining / speed if speed > 0 else 0

        print(
            f"Processed {stats['done']}/{stats['total']} | "
            f"Batch {processed} | "
            f"{elapsed:.2f}s | "
            f"{speed:.1f} chunk/s | "
            f"ETA {eta/60:.1f} min"
        )

        queue.task_done()


async def main():

    client = ollama.AsyncClient()

    async with AsyncSessionLocal() as session:

        result = await session.execute(text("""
            SELECT id, text
            FROM case_chunks
            WHERE embedding IS NULL
              AND karar_no LIKE '2026%'
            ORDER BY id
        """))

        rows = result.fetchall()

    total = len(rows)

    print(f"Toplam embedlenecek chunk: {total}")

    queue = asyncio.Queue()

    stats = {
        "done": 0,
        "total": total,
        "start": time.time()
    }

    for i in range(0, total, BATCH_SIZE):

        batch = rows[i:i+BATCH_SIZE]

        await queue.put(batch)

    workers = []

    for _ in range(WORKER_COUNT):

        workers.append(
            asyncio.create_task(
                worker(queue, client, stats)
            )
        )

    await queue.join()

    for _ in workers:

        await queue.put(None)

    await asyncio.gather(*workers)

    print("Embedding tamamlandı")


if __name__ == "__main__":

    asyncio.run(main())