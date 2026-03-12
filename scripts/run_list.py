import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.crawler_list import fetch_all_pages, fetch_incremental_pages
from app.db import SessionLocal
from app.state_utils import load_state

def main():
    mode = "full"   # "full" veya "incremental"
    start_page = 107
    max_pages = 55000

    start_page = load_state(start_page)


    db = SessionLocal()
    try:
        if mode == "full":
            result = fetch_all_pages(
                db=db,
                start_page=start_page,
                page_size=100,
                sleep_sec=1,
                max_pages=max_pages,
            )
        else:
            result = fetch_incremental_pages(
                db=db,
                page_size=100,
                sleep_sec=1,
                max_pages=max_pages,
            )

        print("-" * 50)
        for key, value in result.items():
            print(f"{key}: {value}")

    finally:
        db.close()


if __name__ == "__main__":
    main()