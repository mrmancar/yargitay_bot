import asyncio
import os
import sys
import time
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

import ollama

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MODEL_NAME = "embeddinggemma"
BATCH_SIZE = 128
CONCURRENT_WORKERS = 4

DATABASE_URL = "postgresql+asyncpg://yargitay_user:123456@localhost/yargitay_bot"

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=20,
    max_overflow=20
)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("embedder")


async def get_total():

    async with AsyncSessionLocal() as session:

        result = await session.execute(text("""
            SELECT count(*)
            FROM case_chunks
            WHERE embedding IS NULL
        """))

        return result.scalar()


async def fetch_batch(session, last_id):

    result = await session.execute(text("""
        SELECT id, text
        FROM case_chunks
        WHERE embedding IS NULL
        AND id > :last_id
        ORDER BY id
        LIMIT :limit
    """), {
        "last_id": last_id,
        "limit": BATCH_SIZE
    })

    return result.fetchall()


async def update_embeddings(rows, embeddings):

    ids = [r.id for r in rows]
    emb = [str(e) for e in embeddings]

    async with AsyncSessionLocal() as session:

        await session.execute(text("""
            UPDATE case_chunks c
            SET embedding = v.embedding::vector
            FROM (
                SELECT
                    unnest(:ids) as id,
                    unnest(:embeddings) as embedding
            ) v
            WHERE c.id = v.id
        """), {
            "ids": ids,
            "embeddings": emb
        })

        await session.commit()




async def worker(queue, client, stats):

    while True:

        batch = await queue.get()

        if batch is None:
            queue.task_done()
            return

        logger.info(f"Worker embedding başladı | batch={len(batch)}")

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
            logger.error(f"Worker error: {e}")
            processed = 0

        elapsed = time.time() - start

        speed = stats["done"] / (time.time() - stats["start"])

        remaining = stats["total"] - stats["done"]

        eta = remaining / speed if speed > 0 else 0

        logger.info(
            f"Processed {stats['done']}/{stats['total']} | "
            f"Batch {processed} | "
            f"{elapsed:.2f}s | "
            f"{speed:.1f} chunk/s | "
            f"ETA {eta/60:.1f} min"
        )

        queue.task_done()





async def producer(queue):

    last_id = 0

    async with AsyncSessionLocal() as session:

        while True:

            rows = await fetch_batch(session, last_id)

            if not rows:
                break

            last_id = rows[-1].id

            await queue.put(rows)

    for _ in range(CONCURRENT_WORKERS):
        await queue.put(None)


async def main():

    total = await get_total()

    logger.info(f"Toplam embedlenecek chunk: {total}")

    queue = asyncio.Queue(maxsize=CONCURRENT_WORKERS * 2)

    client = ollama.AsyncClient()

    stats = {
        "done": 0,
        "total": total,
        "start": time.time()
    }

    workers = [
        asyncio.create_task(worker(queue, client, stats))
        for _ in range(CONCURRENT_WORKERS)
    ]

    prod = asyncio.create_task(producer(queue))

    await prod
    await queue.join()

    for w in workers:
        await w

    total_time = time.time() - stats["start"]

    logger.info(f"Tamamlandı")
    logger.info(f"Toplam süre: {total_time/60:.2f} dakika")


if __name__ == "__main__":
    asyncio.run(main())