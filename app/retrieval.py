# app/retrieval.py

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def _build_search_sql(daire_filter: str | None = None) -> tuple[str, dict[str, Any]]:
    base_sql = """
        SELECT
            id,
            case_id,
            chunk_index,
            chunk_type,
            text,
            daire_text,
            esas_no,
            karar_no,
            tarih,
            1 - (embedding <=> CAST(:query_embedding AS vector)) AS similarity
        FROM case_chunks
        WHERE embedding IS NOT NULL
    """

    params: dict[str, Any] = {}

    if daire_filter:
        base_sql += """
          AND daire_text IS NOT NULL
          AND daire_text ILIKE :daire_filter
        """
        params["daire_filter"] = f"%{daire_filter}%"

    base_sql += """
        ORDER BY embedding <=> CAST(:query_embedding AS vector)
        LIMIT :raw_top_k
    """

    return base_sql, params


def _collapse_duplicates(
    rows: list[dict[str, Any]],
    final_top_k: int,
    max_chunks_per_case: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    case_counter: dict[int, int] = {}

    for row in rows:
        case_id = row["case_id"]
        used_count = case_counter.get(case_id, 0)

        if used_count >= max_chunks_per_case:
            continue

        selected.append(row)
        case_counter[case_id] = used_count + 1

        if len(selected) >= final_top_k:
            break

    return selected


def search_similar_chunks(
    db: Session,
    query_embedding: list[float],
    final_top_k: int = 5,
    raw_top_k: int = 30,
    max_chunks_per_case: int = 1,
    daire_filter: str | None = None,
) -> list[dict[str, Any]]:
    """
    1) pgvector ile ham top-N getirir
    2) aynı case_id tekrarlarını collapse eder
    3) final top-K sonucu döner
    """

    if raw_top_k < final_top_k:
        raw_top_k = final_top_k

    sql, extra_params = _build_search_sql(daire_filter=daire_filter)

    params: dict[str, Any] = {
        "query_embedding": "[" + ",".join(str(x) for x in query_embedding) + "]",
        "raw_top_k": raw_top_k,
        **extra_params,
    }

    rows = db.execute(text(sql), params).mappings().all()
    row_dicts = [dict(row) for row in rows]

    collapsed = _collapse_duplicates(
        rows=row_dicts,
        final_top_k=final_top_k,
        max_chunks_per_case=max_chunks_per_case,
    )

    return collapsed