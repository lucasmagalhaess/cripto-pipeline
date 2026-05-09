# Cripto Pipeline — Preços de Criptomoedas em Tempo Real

Pipeline de dados de criptomoedas usando Cloud Functions, Airflow e BigQuery — arquitetura idêntica à utilizada em times de engenharia de dados de fintechs.

## Arquitetura
## Por que esse projeto é diferente

Nos projelines tradicionais, o processamento roda localmente em Docker com Spark e MinIO. Nesse projeto toda a lógica roda no GCP:

- **Cloud Functions** substituem o Docker — código Python serverless, sem servidor pra gerenciar
- **GCS** é o Data Lake na nuvem — camada bronze com dado bruto preservado
- **Airflow chama URLs HTTP** em vez de executar scripts locais
- **Terraform** provisiona bucket, datasets e tabela com schema definido como código

Esse é o padrão adotado por times de engenharia de dados em fintechs — ETLs em Cloud Functions orquestradas por Airflow.

## Tecnologias

| Tecnologia | Função |
|---|---|
| Python | Lógica das Cloud Functions |
| Google Cloud Functions | Execução serverless das ETLs |
| Google Cloud Storage | Data Lake — camada bronze |
| BigQuery | Data Warehouse — camada gold |
| Apache Airflow | Orquestração via HTTP |
| Terraform | Infraestrutura como código |

## Medallion Architecture

**Bronze (GCS):** Dado bruto exatamente como veio da API — nunca modificado, sempre disponível para reprocessamento.

**Gold (BigQuery):** Dado transformado e modelado, pronto para consumo por analistas e dashboards.

## Cloud Functions

**extract-cripto**
Chama a API da CoinGecko, extrai preços de 10 criptomoedas (BTC, ETH, SOL, XRP e outros) e salva o JSON bruto no GCS na camada bronze.

**transform-cripto**
Lê o JSON bruto do GCS, calcula o preço em BRL, limpa e estrutura os dados e insere no BigQuery.

## DAG do Airflow

```python
extrair_criptomoedas >> transformar_e_carregar_bigquery
```

A DAG roda diariamente às 9h, chama as Cloud Functions via HTTP e passa o nome do arquivo entre as tasks via XCom.

## Queries no BigQuery

```sql
-- Preços atuais ordenados por market cap
SELECT name, symbol, price_usd, price_brl, price_change_24h, price_change_7d
FROM `cripto-pipeline-495818.analytics.cripto_prices`
ORDER BY market_cap_usd DESC;

-- Moedas com maior variação positiva em 7 dias
SELECT name, symbol, price_usd, price_change_7d
FROM `cripto-pipeline-495818.analytics.cripto_prices`
WHERE price_change_7d > 0
ORDER BY price_change_7d DESC;

-- Histórico de preço do Bitcoin
SELECT extraction_date, extraction_timestamp, price_usd, price_brl
FROM `cripto-pipeline-495818.analytics.cripto_prices`
WHERE coin_id = 'bitcoin'
ORDER BY extraction_timestamp DESC;
```

## Como rodar

### 1. Criar infraestrutura GCP
```bash
cd terraform
terraform init
terraform apply
```

### 2. Deploy das Cloud Functions
```bash
gcloud functions deploy extract-cripto \
  --gen2 --runtime=python311 --region=us-central1 \
  --source=functions/extract --entry-point=extract_cripto \
  --trigger-http --allow-unauthenticated

gcloud functions deploy transform-cripto \
  --gen2 --runtime=python311 --region=us-central1 \
  --source=functions/transform --entry-point=transform_cripto \
  --trigger-http --allow-unauthenticated
```

### 3. Subir o Airflow
```bash
cd docker
docker compose up airflow-init
docker compose up -d
```

### 4. Ativar a DAG
Acesse o Airflow em `http://localhost:8086` e ative a DAG `pipeline_cripto`.

## Autor

**Lucas Magalhães** — Engenheiro de Dados

[![GitHub](https://img.shields.io/badge/GitHub-lucasmagalhaess-black)](https://github.com/lucasmagalhaess)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-lucasmagalhaes--data-blue)](https://linkedin.com/in/lucasmagalhaes-data)
