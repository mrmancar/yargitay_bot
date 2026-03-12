import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.client import YargitayClient


def main():
    client = YargitayClient()

    try:
        client.init_session()

        print("Session initialized successfully.")
        print("Cookies:", client.client.cookies)

    finally:
        client.close()


if __name__ == "__main__":
    main()