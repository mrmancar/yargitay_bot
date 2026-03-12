from dotenv import load_dotenv
import os

load_dotenv()


class Settings:

    def __init__(self):

        self.DATABASE_URL = os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg2://postgres:postgres@localhost:5432/yargitay_bot"
        )

        self.BASE_URL = os.getenv(
            "BASE_URL",
            "https://karararama.yargitay.gov.tr"
        )

        self.REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
        self.PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10"))
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

        self.OLLAMA_BASE_URL = os.getenv(
            "OLLAMA_BASE_URL",
            "http://localhost:11434"
        )

        self.EMBED_MODEL = os.getenv(
            "EMBED_MODEL",
            "embeddinggemma"
        )


settings = Settings()