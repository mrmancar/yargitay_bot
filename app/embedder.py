# app/embedder.py

from __future__ import annotations

from typing import Any

import httpx

from app.config import settings


class EmbeddingClient:
    def __init__(self) -> None:
        self.base_url = settings.OLLAMA_BASE_URL
        self.model = settings.EMBED_MODEL

    async def embed(self, text: str) -> list[float]:
        payload = {
            "model": self.model,
            "input": text,
        }

        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(
                f"{self.base_url}/api/embed",
                json=payload,
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()

        embeddings = data.get("embeddings")
        if not embeddings:
            raise ValueError("Embedding response içinde embeddings yok")

        if not isinstance(embeddings, list) or not embeddings[0]:
            raise ValueError("Embedding response formatı beklenenden farklı")

        return embeddings[0]