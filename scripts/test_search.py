import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.client import YargitayClient


def main():
    payload = {
        "data": {
            "arananKelime": "",
            "hukuk": "23. Hukuk Dairesi",
            "esasYil": "",
            "esasIlkSiraNo": "",
            "esasSonSiraNo": "",
            "kararYil": "",
            "kararIlkSiraNo": "",
            "kararSonSiraNo": "",
            "baslangicTarihi": "",
            "bitisTarihi": "",
            "siralama": "3",
            "siralamaDirection": "desc",
            "birimYrgKurulDaire": "",
            "birimYrgHukukDaire": (
                "1. Hukuk Dairesi+2. Hukuk Dairesi+3. Hukuk Dairesi+4. Hukuk Dairesi+"
                "5. Hukuk Dairesi+6. Hukuk Dairesi+7. Hukuk Dairesi+8. Hukuk Dairesi+"
                "9. Hukuk Dairesi+10. Hukuk Dairesi+11. Hukuk Dairesi+12. Hukuk Dairesi+"
                "13. Hukuk Dairesi+14. Hukuk Dairesi+15. Hukuk Dairesi+16. Hukuk Dairesi+"
                "17. Hukuk Dairesi+18. Hukuk Dairesi+19. Hukuk Dairesi+20. Hukuk Dairesi+"
                "21. Hukuk Dairesi+22. Hukuk Dairesi+23. Hukuk Dairesi"
            ),
            "birimYrgCezaDaire": "",
            "pageSize": 100,
            "pageNumber": 1,
        }
    }

    client = YargitayClient()

    try:
        client.init_session()
        result = client.post_search(payload)

        print(json.dumps(result, ensure_ascii=False, indent=2))

    finally:
        client.close()


if __name__ == "__main__":
    main()