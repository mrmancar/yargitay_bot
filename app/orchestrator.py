from app.crawler_detail import fetch_and_save_detail_batch
from app.crawler_list import fetch_and_save_pages
from app.parser import parse_batch


def run_pipeline(
    db,
    start_page: int = 1,
    end_page: int = 1,
    page_size: int = 100,
    list_sleep_sec: float = 1.0,
    detail_limit: int = 10,
    parse_limit: int = 10,
) -> dict:
    list_result = fetch_and_save_pages(
        db=db,
        start_page=start_page,
        end_page=end_page,
        page_size=page_size,
        sleep_sec=list_sleep_sec,
    )

    detail_result = fetch_and_save_detail_batch(
        db=db,
        limit=detail_limit,
    )

    parse_result = parse_batch(
        db=db,
        limit=parse_limit,
    )

    return {
        "list": list_result,
        "detail": detail_result,
        "parse": parse_result,
    }