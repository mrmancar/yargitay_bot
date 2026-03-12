import os
import sys
import time
import traceback
import app.crawler_detail as cd
print("crawler_detail file:", cd.__file__)

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.crawler_detail import fetch_and_save_detail_batch
from app.db import SessionLocal


def main():
    batch_size = 20
    sleep_sec = 60
    target_year = 2026
    concurrency = 5

    db = SessionLocal()

    total_processed = 0
    total_success = 0
    total_failed = 0
    round_no = 0

    try:
        while True:
            round_no += 1

            try:
                result = fetch_and_save_detail_batch(
                    db=db,
                    limit=batch_size,
                    target_year=target_year,
                    concurrency=concurrency,
                )

                processed = result["processed"]
                success = result["success"]
                failed = result["failed"]

            except Exception as exc:
                print("=" * 60)
                print(f"ERROR OCCURRED DURING BATCH round={round_no}")
                print(str(exc))
                traceback.print_exc()
                print(f"Sleeping {sleep_sec} seconds before retry...")
                time.sleep(sleep_sec)
                continue

            total_processed += processed
            total_success += success
            total_failed += failed

            print("-" * 60)
            print(f"Round       : {round_no}")
            print(f"Year        : {target_year}")
            print(f"Concurrency : {concurrency}")
            print(f"Processed   : {processed}")
            print(f"Success     : {success}")
            print(f"Failed      : {failed}")
            print(f"Total proc  : {total_processed}")
            print(f"Total succ  : {total_success}")
            print(f"Total fail  : {total_failed}")

            if processed == 0:
                print(f"No more cases left for detail fetching for year={target_year}.")
                break

            print(f"Sleeping {sleep_sec} seconds before next batch...")
            time.sleep(sleep_sec)

    finally:
        db.close()


if __name__ == "__main__":
    main()