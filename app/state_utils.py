import os

STATE_FILE = ".state"


def load_state(default_page: int) -> int:
    if not os.path.exists(STATE_FILE):
        return default_page

    try:
        with open(STATE_FILE, "r") as f:
            page = int(f.read().strip())
            print(f"Resuming from page {page}")
            return page
    except Exception:
        return default_page


def save_state(page: int):
    with open(STATE_FILE, "w") as f:
        f.write(str(page))