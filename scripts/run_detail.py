import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.crawler_detail import fetch_and_save_detail_batch
from app.db import SessionLocal


def main():
    db = SessionLocal()
    try:
        result = fetch_and_save_detail_batch(
            db=db,
            limit=20,
            target_year=2026,
        )

        print("-" * 40)
        print("DETAIL BATCH RESULT")
        print(f"Processed : {result['processed']}")
        print(f"Success   : {result['success']}")
        print(f"Failed    : {result['failed']}")
    finally:
        db.close()


if __name__ == "__main__":
    main()