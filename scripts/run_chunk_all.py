import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models import ParsedCaseDocument
from sqlalchemy import text

CHUNK_SIZE = 3000
OVERLAP = 500


def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\r", "\n")
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)

    return text.strip()


def split_text(text: str):
    text = normalize_text(text)

    chunks = []

    start = 0
    step = CHUNK_SIZE - OVERLAP

    while start < len(text):
        end = start + CHUNK_SIZE
        chunk = text[start:end].strip()

        if chunk:
            chunks.append(chunk)

        start += step

    return chunks

from datetime import datetime, date


def parse_tarih_value(value):
    if not value:
        return None

    if isinstance(value, date):
        return value

    value = str(value).strip()

    patterns = [
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
    ]

    for fmt in patterns:
        try:
            return datetime.strptime(value[:10], fmt).date()
        except:
            pass

    return None


def build_header(doc):
    parts = []

    if doc.daire_text:
        parts.append(f"Daire: {doc.daire_text}")

    if doc.esas_no:
        parts.append(f"Esas No: {doc.esas_no}")

    if doc.karar_no:
        parts.append(f"Karar No: {doc.karar_no}")

    parsed_tarih = parse_tarih_value(doc.tarihi)
    if parsed_tarih:
        parts.append(f"Tarih: {parsed_tarih}")

    if doc.mahkemesi:
        parts.append(f"Mahkemesi: {doc.mahkemesi}")

    if doc.title:
        parts.append(f"Başlık: {doc.title}")

    return "\n".join(parts).strip()


def run():
    db: Session = SessionLocal()

    docs = db.query(ParsedCaseDocument).all()

    total_chunks = 0

    for doc in docs:

        header_text = build_header(doc)

        body_text = doc.ictihat_metni or doc.clean_text or ""

        chunks = []

        if header_text:
            chunks.append((0, "header", header_text))

        body_chunks = split_text(body_text)

        for i, chunk in enumerate(body_chunks, start=1):
            chunks.append((i, "body", chunk))

        for chunk_index, chunk_type, chunk_text in chunks:

            db.execute(
                text("""
                INSERT INTO case_chunks
                (case_id, chunk_index, chunk_type, text, daire_text, esas_no, karar_no, tarih)
                VALUES
                (:case_id, :chunk_index, :chunk_type, :text, :daire_text, :esas_no, :karar_no, :tarih)
                """),
                {
                    "case_id": doc.case_id,
                    "chunk_index": chunk_index,
                    "chunk_type": chunk_type,
                    "text": chunk_text,
                    "daire_text": doc.daire_text,
                    "esas_no": doc.esas_no,
                    "karar_no": doc.karar_no,
                    "tarih": parse_tarih_value(doc.tarihi),                }
            )

        total_chunks += len(chunks)

        if total_chunks % 500 == 0:
            print("chunks created:", total_chunks)

    db.commit()

    print("TOTAL CHUNKS:", total_chunks)


if __name__ == "__main__":
    run()