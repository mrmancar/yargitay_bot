import os
import sys
import time
import traceback

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.crawler_detail import fetch_and_save_detail_batch
from app.db import SessionLocal


def main():
    target_year = 2026
    batch_size = 20
    cooldown_sec = 60

    total_processed = 0
    total_success = 0
    total_failed = 0
    round_no = 0

    db = SessionLocal()

    try:
        while True:
            round_no += 1

            try:
                result = fetch_and_save_detail_batch(
                    db=db,
                    limit=batch_size,
                    target_year=target_year,
                )

                processed = result["processed"]
                success = result["success"]
                failed = result["failed"]

            except Exception as exc:
                print("=" * 60)
                print(f"BATCH ERROR round={round_no} error={exc}")
                traceback.print_exc()
                print(f"{cooldown_sec} saniye bekleniyor...")
                time.sleep(cooldown_sec)
                continue

            total_processed += processed
            total_success += success
            total_failed += failed

            print("=" * 60)
            print(f"ROUND           : {round_no}")
            print(f"TARGET YEAR     : {target_year}")
            print(f"BATCH PROCESSED : {processed}")
            print(f"BATCH SUCCESS   : {success}")
            print(f"BATCH FAILED    : {failed}")
            print(f"TOTAL PROCESSED : {total_processed}")
            print(f"TOTAL SUCCESS   : {total_success}")
            print(f"TOTAL FAILED    : {total_failed}")

            if processed == 0:
                print(f"{target_year} için indirilecek kayıt kalmadı.")
                break

            print(f"{cooldown_sec} saniye bekleniyor...")
            time.sleep(cooldown_sec)

    finally:
        db.close()


if __name__ == "__main__":
    main()