import asyncio
import time

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.client import AsyncYargitayClient
from app.models import Case, CaseDetail

print("LOADED NEW crawler_detail.py")

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


async def _fetch_one_case(
    client: AsyncYargitayClient,
    case: Case,
    semaphore: asyncio.Semaphore,
) -> tuple[Case, dict | None, Exception | None]:
    async with semaphore:
        started = time.perf_counter()
        print(f"START case_id={case.id}")

        try:
            response_json = await client.get_document(case.id)
            elapsed = time.perf_counter() - started
            print(f"END   case_id={case.id} elapsed={elapsed:.2f}s")
            return case, response_json, None
        except Exception as exc:
            elapsed = time.perf_counter() - started
            print(f"FAIL  case_id={case.id} elapsed={elapsed:.2f}s error={exc}")
            return case, None, exc


async def _fetch_case_details_async(
    cases: list[Case],
    concurrency: int = 5,
) -> list[tuple[Case, dict | None, Exception | None]]:
    semaphore = asyncio.Semaphore(concurrency)
    client = AsyncYargitayClient()
    results: list[tuple[Case, dict | None, Exception | None]] = []

    try:
        await client.init_session()

        tasks = [
            asyncio.create_task(_fetch_one_case(client, case, semaphore))
            for case in cases
        ]

        for completed_task in asyncio.as_completed(tasks):
            result = await completed_task
            results.append(result)

        return results

    finally:
        await client.close()


def fetch_and_save_detail_batch(
    db: Session,
    limit: int = 20,
    target_year: int | None = None,
    concurrency: int = 5,
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

    results = asyncio.run(
        _fetch_case_details_async(
            cases=cases,
            concurrency=concurrency,
        )
    )

    processed = 0
    success = 0
    failed = 0

    for case, response_json, error in results:
        processed += 1

        if error is not None:
            db.rollback()
            print(
                f"FAIL SAVE case_id={case.id} "
                f"karar_tarihi_raw={case.karar_tarihi_raw} "
                f"error={error}"
            )
            failed += 1
            continue

        try:
            save_case_detail(db, case.id, response_json)
            html_text = response_json.get("data", "") or ""
            print(
                f"OK SAVE case_id={case.id} "
                f"karar_tarihi_raw={case.karar_tarihi_raw} "
                f"html_length={len(html_text)}"
            )
            success += 1
        except Exception as exc:
            db.rollback()
            print(
                f"FAIL DB case_id={case.id} "
                f"karar_tarihi_raw={case.karar_tarihi_raw} "
                f"db_error={exc}"
            )
            failed += 1

    return {
        "processed": processed,
        "success": success,
        "failed": failed,
    }