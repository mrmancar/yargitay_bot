import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.db import SessionLocal
from app.orchestrator import run_pipeline


def main():
    db = SessionLocal()

    try:
        result = run_pipeline(
            db=db,
            start_page=1,
            end_page=1,
            page_size=100,
            list_sleep_sec=1.0,
            detail_limit=10,
            parse_limit=10,
        )

        print("\n" + "=" * 50)
        print("PIPELINE RESULT")
        print("=" * 50)

        print("\n[LIST]")
        print(f"Fetched  : {result['list']['total_fetched']}")
        print(f"Inserted : {result['list']['total_inserted']}")
        print(f"Updated  : {result['list']['total_updated']}")

        print("\n[DETAIL]")
        print(f"Processed: {result['detail']['processed']}")
        print(f"Success  : {result['detail']['success']}")
        print(f"Failed   : {result['detail']['failed']}")

        print("\n[PARSE]")
        print(f"Processed: {result['parse']['processed']}")
        print(f"Success  : {result['parse']['success']}")
        print(f"Failed   : {result['parse']['failed']}")

    finally:
        db.close()


if __name__ == "__main__":
    main()