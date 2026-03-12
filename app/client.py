import httpx

from app.config import settings


class YargitayClient:

    def __init__(self) -> None:
        self.client = httpx.Client(
            base_url=settings.BASE_URL,
            timeout=settings.REQUEST_TIMEOUT,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/145.0.0.0 Safari/537.36"
                ),
                "Accept": "*/*",
                "Referer": f"{settings.BASE_URL}/",
                "Origin": settings.BASE_URL,
                "X-Requested-With": "XMLHttpRequest",
            },
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()

    def init_session(self) -> None:
        response = self.client.get("/")
        response.raise_for_status()

    def post_search(self, payload: dict) -> dict:
        response = self.client.post(
            "/aramadetaylist",
            json=payload,
            headers={
                "Content-Type": "application/json; charset=UTF-8",
            },
        )

        response.raise_for_status()
        return response.json()

    def get_document(self, case_id: int | str) -> dict:
        response = self.client.get(
            f"/getDokuman?id={case_id}",
            headers={
                "Accept": "*/*",
                "X-Requested-With": "XMLHttpRequest",
            },
        )

        response.raise_for_status()
        return response.json()