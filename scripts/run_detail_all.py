import sys
import os
import time
import traceback

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.crawler_detail import fetch_and_save_detail_batch
from app.db import SessionLocal


def main():
    batch_size = 20
    sleep_sec = 60

    db = SessionLocal()

    total_processed = 0
    total_success = 0
    total_failed = 0
    round_no = 0

    try:
        while True:
            round_no += 1

            try:
                result = fetch_and_save_detail_batch(db, limit=batch_size)

                processed = result["processed"]
                success = result["success"]
                failed = result["failed"]

            except Exception as e:
                print("=" * 50)
                print("ERROR OCCURRED DURING BATCH")
                print(str(e))
                traceback.print_exc()
                print("Retrying same batch after 60 seconds...")
                time.sleep(sleep_sec)
                continue

            total_processed += processed
            total_success += success
            total_failed += failed

            print("-" * 50)
            print(f"Round       : {round_no}")
            print(f"Processed   : {processed}")
            print(f"Success     : {success}")
            print(f"Failed      : {failed}")
            print(f"Total proc  : {total_processed}")
            print(f"Total succ  : {total_success}")
            print(f"Total fail  : {total_failed}")

            if processed == 0:
                print("No more cases left for detail fetching.")
                break

            print("Sleeping 60 seconds before next batch...")
            time.sleep(sleep_sec)

    finally:
        db.close()


if __name__ == "__main__":
    main()