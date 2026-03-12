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
    idx: int,
) -> tuple[int, Case, dict | None, Exception | None]:
    async with semaphore:
        started = time.perf_counter()
        print(f"[{idx}] START case_id={case.id}")

        try:
            response_json = await client.get_document(case.id)
            elapsed = time.perf_counter() - started
            print(f"[{idx}] END   case_id={case.id} elapsed={elapsed:.2f}s")
            return idx, case, response_json, None
        except Exception as exc:
            elapsed = time.perf_counter() - started
            print(f"[{idx}] FAIL  case_id={case.id} elapsed={elapsed:.2f}s error={exc}")
            return idx, case, None, exc


async def _fetch_case_details_async(
    cases: list[Case],
    concurrency: int = 5,
) -> list[tuple[int, Case, dict | None, Exception | None]]:
    semaphore = asyncio.Semaphore(concurrency)
    client = AsyncYargitayClient()
    results: list[tuple[int, Case, dict | None, Exception | None]] = []

    try:
        await client.init_session()

        tasks = [
            asyncio.create_task(_fetch_one_case(client, case, semaphore, idx))
            for idx, case in enumerate(cases, start=1)
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
    batch_started = time.perf_counter()

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
            "fetch_seconds": 0.0,
            "save_seconds": 0.0,
            "total_seconds": 0.0,
        }

    fetch_started = time.perf_counter()
    results = asyncio.run(
        _fetch_case_details_async(
            cases=cases,
            concurrency=concurrency,
        )
    )
    fetch_seconds = time.perf_counter() - fetch_started

    processed = 0
    success = 0
    failed = 0

    save_started = time.perf_counter()

    for idx, case, response_json, error in results:
        processed += 1

        if error is not None:
            db.rollback()
            print(
                f"[{idx}] FAIL SAVE case_id={case.id} "
                f"karar_tarihi_raw={case.karar_tarihi_raw} "
                f"error={error}"
            )
            failed += 1
            continue

        try:
            save_case_detail(db, case.id, response_json)
            html_text = response_json.get("data", "") or ""
            print(
                f"[{idx}] OK SAVE case_id={case.id} "
                f"karar_tarihi_raw={case.karar_tarihi_raw} "
                f"html_length={len(html_text)}"
            )
            success += 1
        except Exception as exc:
            db.rollback()
            print(
                f"[{idx}] FAIL DB case_id={case.id} "
                f"karar_tarihi_raw={case.karar_tarihi_raw} "
                f"db_error={exc}"
            )
            failed += 1

    save_seconds = time.perf_counter() - save_started
    total_seconds = time.perf_counter() - batch_started

    print("=" * 60)
    print(
        f"BATCH TIMING "
        f"concurrency={concurrency} "
        f"fetch={fetch_seconds:.2f}s "
        f"save={save_seconds:.2f}s "
        f"total={total_seconds:.2f}s"
    )
    print("=" * 60)

    return {
        "processed": processed,
        "success": success,
        "failed": failed,
        "fetch_seconds": round(fetch_seconds, 2),
        "save_seconds": round(save_seconds, 2),
        "total_seconds": round(total_seconds, 2),
    }