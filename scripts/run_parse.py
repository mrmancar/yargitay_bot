import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.db import SessionLocal
from app.parser import parse_batch


def main():
    db = SessionLocal()

    try:
        result = parse_batch(db, limit=10)

        print("-" * 40)
        print(f"Processed : {result['processed']}")
        print(f"Success   : {result['success']}")
        print(f"Failed    : {result['failed']}")

    finally:
        db.close()


if __name__ == "__main__":
    main()