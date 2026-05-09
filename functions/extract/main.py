import requests
import json
import functions_framework
from datetime import datetime
from google.cloud import storage

GCS_BUCKET = "cripto-pipeline-495818-data-lake"

COINS = [
    "bitcoin", "ethereum", "binancecoin", "solana", "ripple",
    "cardano", "avalanche-2", "polkadot", "chainlink", "uniswap"
]

def get_cripto_prices():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "24h,7d"
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def save_to_gcs(data, filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json"
    )
    print(f"Salvo no GCS: {filename}")

@functions_framework.http
def extract_cripto(request):
    try:
        now = datetime.utcnow()
        today = now.strftime("%Y-%m-%d")
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%S")

        print(f"Iniciando extracao de criptomoedas...")
        coins_data = get_cripto_prices()

        payload = {
            "extraction_date": today,
            "extraction_timestamp": timestamp,
            "total_coins": len(coins_data),
            "coins": coins_data
        }

        filename = f"bronze/cripto/{today}/prices_{timestamp.replace(':', '-')}.json"
        save_to_gcs(payload, filename)

        print(f"Extracao concluida! {len(coins_data)} moedas extraidas.")
        return {"status": "success", "coins": len(coins_data), "file": filename}, 200

    except Exception as e:
        print(f"Erro: {e}")
        return {"status": "error", "message": str(e)}, 500
