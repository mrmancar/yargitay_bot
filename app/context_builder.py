# app/context_builder.py

from __future__ import annotations

from typing import Any


def _clean_text(value: str | None) -> str:
    if not value:
        return ""

    return " ".join(value.strip().split())


def format_chunk_for_context(item: dict[str, Any], rank: int) -> str:
    daire = _clean_text(item.get("daire_text"))
    esas_no = _clean_text(item.get("esas_no"))
    karar_no = _clean_text(item.get("karar_no"))
    tarih = _clean_text(item.get("tarih"))
    chunk_type = _clean_text(item.get("chunk_type"))
    text_value = _clean_text(item.get("text"))
    similarity = item.get("similarity")

    similarity_text = ""
    if similarity is not None:
        similarity_text = f"{float(similarity):.6f}"

    parts: list[str] = [
        f"[KARAR {rank}]",
        f"Daire: {daire or '-'}",
        f"Esas No: {esas_no or '-'}",
        f"Karar No: {karar_no or '-'}",
        f"Tarih: {tarih or '-'}",
        f"Chunk Type: {chunk_type or '-'}",
        f"Benzerlik: {similarity_text or '-'}",
        "Metin:",
        text_value or "-",
    ]

    return "\n".join(parts)


def build_rag_context(
    items: list[dict[str, Any]],
    max_items: int | None = None,
) -> str:
    if not items:
        return ""

    selected = items if max_items is None else items[:max_items]

    blocks = [
        format_chunk_for_context(item, rank=i)
        for i, item in enumerate(selected, start=1)
    ]

    return "\n\n" + ("\n\n".join(blocks)) + "\n"