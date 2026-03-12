import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.db import SessionLocal
from app.parser import parse_batch


def main():
    batch_size = 100
    sleep_sec = 0.5

    db = SessionLocal()

    total_processed = 0
    total_success = 0
    total_failed = 0
    round_no = 0

    try:
        while True:
            round_no += 1
            result = parse_batch(db, limit=batch_size)

            processed = result["processed"]
            success = result["success"]
            failed = result["failed"]

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
                print("No more cases left for parsing.")
                break

            time.sleep(sleep_sec)

    finally:
        db.close()


if __name__ == "__main__":
    main()