import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.db import Base, engine
import app.models  # noqa


def main():
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully.")


if __name__ == "__main__":
    main()