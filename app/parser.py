import re

from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Case, CaseDetail, ParsedCaseDocument


def normalize_spaces(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_title_fields(title: str | None) -> tuple[str | None, str | None, str | None]:
    if not title:
        return None, None, None

    normalized = normalize_spaces(title)

    pattern = r"^(.*?)\s+(\d{4}/\d+)\s+E\.\s*,\s*(\d{4}/\d+)\s+K\.$"
    match = re.match(pattern, normalized)

    if not match:
        return normalized, None, None

    daire_text = match.group(1).strip()
    esas_no = match.group(2).strip()
    karar_no = match.group(3).strip()

    return daire_text, esas_no, karar_no


def html_to_clean_text(html: str) -> tuple[str | None, str]:
    soup = BeautifulSoup(html, "html.parser")

    title = None
    b_tag = soup.find("b")
    if b_tag:
        title = b_tag.get_text(" ", strip=True)
        title = normalize_spaces(title)

    text = soup.get_text("\n", strip=True)

    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]

    clean_text = "\n".join(lines)
    clean_text = normalize_spaces(clean_text).replace(" \n", "\n").replace("\n ", "\n")

    return title, clean_text


def get_cases_ready_for_parse(db: Session, limit: int = 10) -> list[tuple[Case, CaseDetail]]:
    stmt = (
        select(Case, CaseDetail)
        .join(CaseDetail, Case.id == CaseDetail.case_id)
        .where(Case.detail_fetched.is_(True))
        .where(Case.parsed.is_(False))
        .order_by(Case.id.asc())
        .limit(limit)
    )

    return list(db.execute(stmt).all())

def save_parsed_document(
    db: Session,
    case_id: int,
    title: str | None,
    clean_text: str,
) -> None:
    normalized_title = normalize_spaces(title) if title else None
    daire_text, esas_no, karar_no = parse_title_fields(normalized_title)
    parsed_fields = parse_document_fields(clean_text)

    existing_doc = db.get(ParsedCaseDocument, case_id)

    if existing_doc:
        existing_doc.title = normalized_title
        existing_doc.daire_text = daire_text
        existing_doc.esas_no = esas_no
        existing_doc.karar_no = karar_no
        existing_doc.mahkemesi = parsed_fields["mahkemesi"]
        existing_doc.tarihi = parsed_fields["tarihi"]
        existing_doc.numarasi = parsed_fields["numarasi"]
        existing_doc.ictihat_metni = parsed_fields["ictihat_metni"]
        existing_doc.clean_text = clean_text
    else:
        new_doc = ParsedCaseDocument(
            case_id=case_id,
            title=normalized_title,
            daire_text=daire_text,
            esas_no=esas_no,
            karar_no=karar_no,
            mahkemesi=parsed_fields["mahkemesi"],
            tarihi=parsed_fields["tarihi"],
            numarasi=parsed_fields["numarasi"],
            ictihat_metni=parsed_fields["ictihat_metni"],
            clean_text=clean_text,
        )
        db.add(new_doc)

    existing_case = db.get(Case, case_id)
    if existing_case:
        existing_case.parsed = True

    db.commit()

def parse_batch(db: Session, limit: int = 10) -> dict:
    rows = get_cases_ready_for_parse(db, limit=limit)

    processed = 0
    success = 0
    failed = 0

    for case, case_detail in rows:
        processed += 1

        try:
            html = case_detail.raw_text or ""
            title, clean_text = html_to_clean_text(html)

            save_parsed_document(
                db=db,
                case_id=case.id,
                title=title,
                clean_text=clean_text,
            )

            print(f"OK   case_id={case.id} text_length={len(clean_text)}")
            success += 1

        except Exception as exc:
            db.rollback()
            print(f"FAIL case_id={case.id} error={exc}")
            failed += 1

    return {
        "processed": processed,
        "success": success,
        "failed": failed,
    }



def extract_labeled_field(text: str, label: str) -> str | None:
    pattern = rf"{label}\s*[:：]\s*(.+)"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None

    value = match.group(1).strip()
    value = normalize_spaces(value)
    return value or None


def extract_ictihat_metni(text: str) -> str | None:
    marker = "İçtihat Metni"
    idx = text.find(marker)

    if idx == -1:
        return None

    result = text[idx + len(marker):].strip()
    result = normalize_spaces(result)
    return result or None


def parse_document_fields(clean_text: str) -> dict:
    return {
        "mahkemesi": extract_labeled_field(clean_text, "MAHKEMESİ"),
        "tarihi": extract_labeled_field(clean_text, "TARİHİ"),
        "numarasi": extract_labeled_field(clean_text, "NUMARASI"),
        "ictihat_metni": extract_ictihat_metni(clean_text),
    }