# import sys
# import os

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# import ollama
# import time
# from sqlalchemy import text
# from app.db import SessionLocal

# MODEL_NAME = "qwen3-embedding"
# BATCH_SIZE = 50


# def main():
#     db = SessionLocal()

#     rows = db.execute(text("""
#         SELECT id, text
#         FROM case_chunks
#         WHERE embedding IS NULL
#         ORDER BY id
#     """)).fetchall()

#     total = len(rows)
#     print(f"Toplam embedlenecek chunk: {total}")

#     done = 0

#     for i in range(0, total, BATCH_SIZE):

#         batch_start = time.time()

#         batch = rows[i:i+BATCH_SIZE]

#         texts = [r.text or "" for r in batch]

#         try:

#             response = ollama.embed(
#                 model=MODEL_NAME,
#                 input=texts
#             )

#             embeddings = response.embeddings

#             for row, embedding in zip(batch, embeddings):

#                 db.execute(
#                     text("""
#                         UPDATE case_chunks
#                         SET embedding = CAST(:embedding AS vector)
#                         WHERE id = :id
#                     """),
#                     {
#                         "id": row.id,
#                         "embedding": str(embedding)
#                     }
#                 )

#                 done += 1

#             db.commit()

#             elapsed = time.time() - batch_start
#             print(f"embed tamam: {done}/{total} | batch süresi: {elapsed:.2f} sn")

#         except Exception as e:

#             db.rollback()
#             print("HATA:", e)

#     print("BİTTİ")


# if __name__ == "__main__":
#     main()


import asyncio
import os
import sys
import time
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import ollama

# Path ayarı (mevcut kodundan alındı)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Yapılandırma
MODEL_NAME = "qwen3-embedding"
BATCH_SIZE = 50
# ÖRNEK ASYNC URL: postgresql+asyncpg://user:pass@localhost/dbname
DATABASE_URL = "postgresql+asyncpg://yargitay_user:123456@localhost/yargitay_bot"

# Async Engine ve Session Oluşturma
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def process_batch(session: AsyncSession, batch, client: ollama.AsyncClient):
    """Tek bir batch'i asenkron olarak işler."""
    texts = [r.text or "" for r in batch]
    
    try:
        # 1. Ollama'dan asenkron embedding al
        response = await client.embed(model=MODEL_NAME, input=texts)
        embeddings = response.embeddings

        # 2. Veritabanını güncelle
        for row, embedding in zip(batch, embeddings):
            await session.execute(
                text("""
                    UPDATE case_chunks 
                    SET embedding = CAST(:embedding AS vector) 
                    WHERE id = :id
                """),
                {"id": row.id, "embedding": str(embedding)}
            )
        
        await session.commit()
        return len(batch)
    
    except Exception as e:
        await session.rollback()
        print(f"Batch Hatası: {e}")
        return 0

async def main():
    client = ollama.AsyncClient() # Ollama Async İstemcisi
    
    async with AsyncSessionLocal() as session:
        # İşlenecek satırları çek
        result = await session.execute(text("""
            SELECT id, text 
            FROM case_chunks 
            WHERE embedding IS NULL 
            ORDER BY id
        """))
        rows = result.fetchall()
        
        total = len(rows)
        print(f"Toplam embedlenecek chunk: {total}")
        
        done = 0
        start_time = time.time()

        for i in range(0, total, BATCH_SIZE):
            batch_start = time.time()
            batch = rows[i : i + BATCH_SIZE]
            
            # Batch işlemini başlat
            processed_count = await process_batch(session, batch, client)
            
            done += processed_count
            elapsed = time.time() - batch_start
            print(f"İlerleme: {done}/{total} | Batch Süresi: {elapsed:.2f}s")

    print(f"Tüm işlem tamamlandı. Toplam süre: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    asyncio.run(main())