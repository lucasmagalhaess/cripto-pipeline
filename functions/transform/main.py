import json
import functions_framework
from datetime import datetime, timezone
from google.cloud import storage, bigquery

GCS_BUCKET = "cripto-pipeline-495818-data-lake"
BQ_PROJECT = "cripto-pipeline-495818"
BQ_DATASET = "analytics"
BQ_TABLE = "cripto_prices"

def read_from_gcs(filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    return json.loads(blob.download_as_string())

def transform_coin(coin, extraction_date, extraction_timestamp):
    price_change_24h = coin.get("price_change_percentage_24h")
    price_change_7d = coin.get("price_change_percentage_7d_in_currency")
    ath = coin.get("ath")
    ath_change = coin.get("ath_change_percentage")
    market_cap = coin.get("market_cap")
    volume = coin.get("total_volume")
    price_usd = coin.get("current_price")

    price_brl = price_usd * 5.75 if price_usd else None

    return {
        "coin_id": coin.get("id"),
        "symbol": coin.get("symbol", "").upper(),
        "name": coin.get("name"),
        "price_usd": float(price_usd) if price_usd else None,
        "price_brl": float(round(price_brl, 2)) if price_brl else None,
        "market_cap_usd": float(market_cap) if market_cap else None,
        "volume_24h_usd": float(volume) if volume else None,
        "price_change_24h": float(round(price_change_24h, 2)) if price_change_24h else None,
        "price_change_7d": float(round(price_change_7d, 2)) if price_change_7d else None,
        "ath_usd": float(ath) if ath else None,
        "ath_change_percent": float(round(ath_change, 2)) if ath_change else None,
        "extraction_date": extraction_date,
        "extraction_timestamp": extraction_timestamp
    }

def load_to_bigquery(rows):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise Exception(f"Erros ao inserir no BigQuery: {errors}")
    print(f"Inseridos {len(rows)} registros no BigQuery")

@functions_framework.http
def transform_cripto(request):
    try:
        request_json = request.get_json(silent=True)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = request_json.get("filename") if request_json else None

        if not filename:
            filename = f"bronze/cripto/{today}/test.json"

        print(f"Lendo arquivo: {filename}")
        raw_data = read_from_gcs(filename)

        coins = raw_data.get("coins", [])
        extraction_date = raw_data.get("extraction_date", today)
        extraction_timestamp = raw_data.get("extraction_timestamp", "")

        print(f"Transformando {len(coins)} moedas...")
        rows = [transform_coin(coin, extraction_date, extraction_timestamp) for coin in coins]

        load_to_bigquery(rows)

        return {"status": "success", "rows_inserted": len(rows)}, 200

    except Exception as e:
        print(f"Erro: {e}")
        return {"status": "error", "message": str(e)}, 500
