from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

EXTRACT_URL = "https://us-central1-cripto-pipeline-495818.cloudfunctions.net/extract-cripto"
TRANSFORM_URL = "https://us-central1-cripto-pipeline-495818.cloudfunctions.net/transform-cripto"

default_args = {
    'owner': 'lucas',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_cripto',
    default_args=default_args,
    description='Pipeline de criptomoedas via Cloud Functions + BigQuery',
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['cripto', 'cloud-functions', 'bigquery', 'gcp'],
) as dag:

    def extract_task(**context):
        print(f"Chamando Cloud Function: {EXTRACT_URL}")
        response = requests.get(EXTRACT_URL, timeout=120)
        response.raise_for_status()
        result = response.json()
        print(f"Resultado: {result}")

        if result.get("status") != "success":
            raise Exception(f"Extracao falhou: {result}")

        filename = result["file"]
        context['task_instance'].xcom_push(key='filename', value=filename)
        print(f"Arquivo gerado: {filename}")
        return filename

    def transform_task(**context):
        filename = context['task_instance'].xcom_pull(key='filename', task_ids='extrair_criptomoedas')
        print(f"Chamando Cloud Function: {TRANSFORM_URL}")
        print(f"Arquivo: {filename}")

        response = requests.post(
            TRANSFORM_URL,
            json={"filename": filename},
            timeout=120
        )
        response.raise_for_status()
        result = response.json()
        print(f"Resultado: {result}")

        if result.get("status") != "success":
            raise Exception(f"Transformacao falhou: {result}")

        print(f"Registros inseridos no BigQuery: {result['rows_inserted']}")
        return result

    t1 = PythonOperator(
        task_id='extrair_criptomoedas',
        python_callable=extract_task,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='transformar_e_carregar_bigquery',
        python_callable=transform_task,
        provide_context=True,
    )

    t1 >> t2
