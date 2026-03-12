# import copy
# import time

# from sqlalchemy.orm import Session

# from app.client import YargitayClient
# from app.models import Case


# DEFAULT_PAYLOAD = {
#     "data": {
#         "arananKelime": "",
#         "hukuk": "23. Hukuk Dairesi",
#         "esasYil": "",
#         "esasIlkSiraNo": "",
#         "esasSonSiraNo": "",
#         "kararYil": "",
#         "kararIlkSiraNo": "",
#         "kararSonSiraNo": "",
#         "baslangicTarihi": "",
#         "bitisTarihi": "",
#         "siralama": "3",
#         "siralamaDirection": "desc",
#         "birimYrgKurulDaire": "",
#         "birimYrgHukukDaire": (
#             "1. Hukuk Dairesi+2. Hukuk Dairesi+3. Hukuk Dairesi+4. Hukuk Dairesi+"
#             "5. Hukuk Dairesi+6. Hukuk Dairesi+7. Hukuk Dairesi+8. Hukuk Dairesi+"
#             "9. Hukuk Dairesi+10. Hukuk Dairesi+11. Hukuk Dairesi+12. Hukuk Dairesi+"
#             "13. Hukuk Dairesi+14. Hukuk Dairesi+15. Hukuk Dairesi+16. Hukuk Dairesi+"
#             "17. Hukuk Dairesi+18. Hukuk Dairesi+19. Hukuk Dairesi+20. Hukuk Dairesi+"
#             "21. Hukuk Dairesi+22. Hukuk Dairesi+23. Hukuk Dairesi"
#         ),
#         "birimYrgCezaDaire": "",
#         "pageSize": 100,
#         "pageNumber": 1,
#     }
# }


# def fetch_case_list(payload: dict | None = None) -> dict:
#     payload = payload or DEFAULT_PAYLOAD

#     client = YargitayClient()
#     try:
#         client.init_session()
#         return client.post_search(payload)
#     finally:
#         client.close()


# def save_case_list(db: Session, response_json: dict) -> tuple[int, int]:
#     rows = response_json.get("data", {}).get("data", [])

#     inserted = 0
#     updated = 0

#     for row in rows:
#         case_id = row.get("id")
#         if not case_id:
#             continue

#         case_id = int(case_id)
#         existing_case = db.get(Case, case_id)

#         if existing_case:
#             existing_case.daire = row.get("daire")
#             existing_case.esas_no = row.get("esasNo")
#             existing_case.karar_no = row.get("kararNo")
#             existing_case.karar_tarihi_raw = row.get("kararTarihi")
#             existing_case.aranan_kelime = row.get("arananKelime")
#             existing_case.source_list_json = row
#             updated += 1
#         else:
#             new_case = Case(
#                 id=case_id,
#                 daire=row.get("daire"),
#                 esas_no=row.get("esasNo"),
#                 karar_no=row.get("kararNo"),
#                 karar_tarihi_raw=row.get("kararTarihi"),
#                 aranan_kelime=row.get("arananKelime"),
#                 source_list_json=row,
#             )
#             db.add(new_case)
#             inserted += 1

#     db.commit()
#     return inserted, updated

# def build_payload(page_number: int, page_size: int = 100) -> dict:
#     payload = copy.deepcopy(DEFAULT_PAYLOAD)
#     payload["data"]["pageNumber"] = page_number
#     payload["data"]["pageSize"] = page_size
#     return payload


# def fetch_and_save_pages(db: Session, start_page: int = 1, end_page: int = 3, page_size: int = 100, sleep_sec: float = 1.0) -> dict:
#     total_fetched = 0
#     total_inserted = 0
#     total_updated = 0

#     for page_number in range(start_page, end_page + 1):
#         payload = build_payload(page_number=page_number, page_size=page_size)
#         response_json = fetch_case_list(payload)

#         rows = response_json.get("data", {}).get("data", [])
#         inserted, updated = save_case_list(db, response_json)

#         total_fetched += len(rows)
#         total_inserted += inserted
#         total_updated += updated

#         print(
#             f"Page {page_number} -> "
#             f"fetched={len(rows)} inserted={inserted} updated={updated}"
#         )

#         time.sleep(sleep_sec)

#     return {
#         "total_fetched": total_fetched,
#         "total_inserted": total_inserted,
#         "total_updated": total_updated,
#     }

import copy
import time
import os
import sys

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.client import YargitayClient
from app.models import Case
from app.state_utils import save_state

DEFAULT_PAYLOAD = {
    "data": {
        "arananKelime": "",
        "hukuk": "23. Hukuk Dairesi",
        "esasYil": "",
        "esasIlkSiraNo": "",
        "esasSonSiraNo": "",
        "kararYil": "",
        "kararIlkSiraNo": "",
        "kararSonSiraNo": "",
        "baslangicTarihi": "",
        "bitisTarihi": "",
        "siralama": "3",
        "siralamaDirection": "desc",
        "birimYrgKurulDaire": "",
        "birimYrgHukukDaire": (
            "1. Hukuk Dairesi+2. Hukuk Dairesi+3. Hukuk Dairesi+4. Hukuk Dairesi+"
            "5. Hukuk Dairesi+6. Hukuk Dairesi+7. Hukuk Dairesi+8. Hukuk Dairesi+"
            "9. Hukuk Dairesi+10. Hukuk Dairesi+11. Hukuk Dairesi+12. Hukuk Dairesi+"
            "13. Hukuk Dairesi+14. Hukuk Dairesi+15. Hukuk Dairesi+16. Hukuk Dairesi+"
            "17. Hukuk Dairesi+18. Hukuk Dairesi+19. Hukuk Dairesi+20. Hukuk Dairesi+"
            "21. Hukuk Dairesi+22. Hukuk Dairesi+23. Hukuk Dairesi"
        ),
        "birimYrgCezaDaire": "",
        "pageSize": 100,
        "pageNumber": 1,
    }
}


def build_payload(page_number: int, page_size: int = 100) -> dict:
    payload = copy.deepcopy(DEFAULT_PAYLOAD)
    payload["data"]["pageNumber"] = page_number
    payload["data"]["pageSize"] = page_size
    return payload


def fetch_case_list(payload: dict | None = None, retries: int = 4, retry_sleep: float = 15.0) -> dict | None:
    payload = payload or DEFAULT_PAYLOAD

    last_error = None

    for attempt in range(1, retries + 1):
        client = YargitayClient()
        try:
            client.init_session()
            return client.post_search(payload)
        except Exception as exc:
            last_error = exc
            print(f"fetch_case_list attempt={attempt} failed: {exc}")
            time.sleep(retry_sleep)
        finally:
            client.close()

    print(f"fetch_case_list failed after {retries} attempts. payload={payload}")
    return None


def get_db_case_count(db: Session) -> int:
    stmt = select(func.count()).select_from(Case)
    return db.execute(stmt).scalar_one()


def get_api_total_count(response_json: dict) -> int:
    return extract_api_total(response_json)


def save_case_list(db: Session, response_json: dict) -> tuple[int, int]:
    rows = extract_rows(response_json)

    inserted = 0
    updated = 0

    for row in rows:
        case_id = row.get("id")
        if not case_id:
            continue

        case_id = int(case_id)
        existing_case = db.get(Case, case_id)

        if existing_case:
            updated += 1
        else:
            new_case = Case(
                id=case_id,
                daire=row.get("daire"),
                esas_no=row.get("esasNo"),
                karar_no=row.get("kararNo"),
                karar_tarihi_raw=row.get("kararTarihi"),
                aranan_kelime=row.get("arananKelime"),
                source_list_json=row,
            )
            db.add(new_case)
            inserted += 1

    db.commit()
    return inserted, updated

def fetch_all_pages(
    db: Session,
    start_page: int = 1,
    page_size: int = 100,
    sleep_sec: float = 1.0,
    max_pages: int | None = None,
) -> dict:
    total_fetched = 0
    total_inserted = 0
    total_updated = 0
    page_number = start_page
    api_total = None

    while True:
        if max_pages is not None and page_number > max_pages:
            break

        payload = build_payload(page_number=page_number, page_size=page_size)
        response_json = fetch_case_list(payload)

        if page_number % 20 == 1:
            print("20 page completed -> waiting 60 seconds...")
            time.sleep(60)

        metadata = response_json.get("metadata", {}) if isinstance(response_json, dict) else {}
        fmte = metadata.get("FMTE", "")

        if "DisplayCaptcha" in fmte:
            print(f"Page {page_number} -> captcha detected, waiting 60 seconds and restarting...")
            print(response_json)

            save_state(page_number)

            time.sleep(60)
            os.execv(sys.executable, [sys.executable] + sys.argv)

        rows = extract_rows(response_json)

        if api_total is None:
            api_total = extract_api_total(response_json)

        if not rows:
            print(f"Page {page_number} -> empty/invalid response, stopping.")
            print(response_json)

            save_state(page_number)

            time.sleep(30)
            os.execv(sys.executable, [sys.executable] + sys.argv)

        inserted, updated = save_case_list(db, response_json)

        total_fetched += len(rows)
        total_inserted += inserted
        total_updated += updated

        print(
            f"Page {page_number} -> "
            f"fetched={len(rows)} inserted={inserted} updated={updated}"
        )

        if len(rows) < page_size:
            print(f"Page {page_number} -> last partial page, stopping.")
            save_state(page_number + 1)
            break

        page_number += 1
        save_state(page_number)
        time.sleep(sleep_sec)

    return {
        "mode": "full",
        "api_total": api_total or 0,
        "total_fetched": total_fetched,
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "last_page": page_number,
    }

def fetch_incremental_pages(
    db: Session,
    page_size: int = 100,
    sleep_sec: float = 1.0,
    max_pages: int = 50,
) -> dict:
    total_fetched = 0
    total_inserted = 0
    total_updated = 0

    first_response = fetch_case_list(build_payload(page_number=1, page_size=page_size))
    api_total = extract_api_total(first_response)
    db_total = get_db_case_count(db)

    print(f"API total: {api_total}")
    print(f"DB total : {db_total}")

    if db_total >= api_total and api_total > 0:
        print("DB count matches API total. No need to scan older pages.")
        return {
            "mode": "incremental",
            "api_total": api_total,
            "db_total": db_total,
            "total_fetched": 0,
            "total_inserted": 0,
            "total_updated": 0,
            "last_page": 0,
            "stopped_reason": "db_count_matches_api_total",
        }

    page_number = 1
    while page_number <= max_pages:
        if page_number == 1:
            response_json = first_response
        else:
            response_json = fetch_case_list(build_payload(page_number=page_number, page_size=page_size))

        rows = extract_rows(response_json)

        if not rows:
            print(f"Page {page_number} -> empty/invalid response, stopping.")
            break

        inserted, updated = save_case_list(db, response_json)

        total_fetched += len(rows)
        total_inserted += inserted
        total_updated += updated

        print(
            f"Page {page_number} -> "
            f"fetched={len(rows)} inserted={inserted} updated={updated}"
        )

        if inserted == 0 and updated == len(rows):
            print(f"Page {page_number} -> all rows already exist. Stopping.")
            return {
                "mode": "incremental",
                "api_total": api_total,
                "db_total": db_total,
                "total_fetched": total_fetched,
                "total_inserted": total_inserted,
                "total_updated": total_updated,
                "last_page": page_number,
                "stopped_reason": "full_page_already_in_db",
            }

        if len(rows) < page_size:
            print(f"Page {page_number} -> last partial page, stopping.")
            break

        page_number += 1
        time.sleep(sleep_sec)

    return {
        "mode": "incremental",
        "api_total": api_total,
        "db_total": db_total,
        "total_fetched": total_fetched,
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "last_page": page_number,
        "stopped_reason": "max_pages_or_last_page",
    }

def extract_rows(response_json: dict | None) -> list[dict]:
    if not isinstance(response_json, dict):
        return []

    data_section = response_json.get("data")
    if not isinstance(data_section, dict):
        return []

    rows = data_section.get("data")
    if not isinstance(rows, list):
        return []

    return rows


def extract_api_total(response_json: dict | None) -> int:
    if not isinstance(response_json, dict):
        return 0

    data_section = response_json.get("data")
    if not isinstance(data_section, dict):
        return 0

    value = data_section.get("recordsTotal", 0)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0