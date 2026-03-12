from sqlalchemy import select
from sqlalchemy.orm import Session

from app.client import YargitayClient
from app.models import Case, CaseDetail


def get_next_case_without_detail(db: Session) -> Case | None:
    stmt = (
        select(Case)
        .where(Case.detail_fetched.is_(False))
        .order_by(Case.id.asc())
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none()


def get_cases_without_detail(
    db: Session,
    limit: int = 20,
    target_year: int | None = None,
) -> list[Case]:
    stmt = select(Case).where(Case.detail_fetched.is_(False))

    if target_year is not None:
        stmt = stmt.where(Case.karar_tarihi_raw.like(f"%.%.{target_year}"))

    stmt = stmt.order_by(Case.id.desc()).limit(limit)

    return list(db.execute(stmt).scalars().all())


def fetch_case_detail(case_id: int) -> dict:
    client = YargitayClient()
    try:
        client.init_session()
        return client.get_document(case_id)
    finally:
        client.close()


def save_case_detail(db: Session, case_id: int, response_json: dict) -> None:
    html_text = response_json.get("data")

    existing_detail = db.get(CaseDetail, case_id)
    if existing_detail:
        existing_detail.raw_response = response_json
        existing_detail.raw_text = html_text
    else:
        db.add(
            CaseDetail(
                case_id=case_id,
                raw_response=response_json,
                raw_text=html_text,
            )
        )

    existing_case = db.get(Case, case_id)
    if existing_case:
        existing_case.detail_fetched = True

    db.commit()


def fetch_and_save_detail_batch(
    db: Session,
    limit: int = 20,
    target_year: int | None = None,
) -> dict:
    cases = get_cases_without_detail(
        db=db,
        limit=limit,
        target_year=target_year,
    )

    if not cases:
        return {
            "processed": 0,
            "success": 0,
            "failed": 0,
        }

    processed = 0
    success = 0
    failed = 0

    client = YargitayClient()

    try:
        client.init_session()

        for case in cases:
            processed += 1
            try:
                response_json = client.get_document(case.id)
                save_case_detail(db, case.id, response_json)

                html_text = response_json.get("data", "") or ""
                print(
                    f"OK case_id={case.id} "
                    f"karar_tarihi_raw={case.karar_tarihi_raw} "
                    f"html_length={len(html_text)}"
                )
                success += 1

            except Exception as exc:
                db.rollback()
                print(
                    f"FAIL case_id={case.id} "
                    f"karar_tarihi_raw={case.karar_tarihi_raw} "
                    f"error={exc}"
                )
                failed += 1
    finally:
        client.close()

    return {
        "processed": processed,
        "success": success,
        "failed": failed,
    }
    cases = get_cases_without_detail(db, limit=limit)

    processed = 0
    success = 0
    failed = 0

    for case in cases:
        processed += 1
        try:
            response_json = fetch_case_detail(case.id)
            save_case_detail(db, case.id, response_json)

            html_text = response_json.get("data", "")
            print(f"OK   case_id={case.id} html_length={len(html_text)}")
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