from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
import os
import requests
import pandas as pd

# Variables de entorno
bucket = os.getenv("GCS_BUCKET_NAME")
project_id = os.getenv("BIGQUERY_PROJECT_ID")
dataset_id = os.getenv("BIGQUERY_DATASET_ID")
table_id = os.getenv("BIGQUERY_TABLE_ID")

# 1️⃣ Tarea para obtener clientes
def fetch_customers():
    records = []
    for _ in range(10):
        user = requests.get('https://randomuser.me/api/').json()['results'][0]
        records.append({
            'full_name': f"{user['name']['first']} {user['name']['last']}",
            'email': user['email'],
            'national_id': user['id']['value'],
            'country': user['location']['country']
        })
    df = pd.DataFrame(records)
    os.makedirs('/opt/airflow/data', exist_ok=True)
    df.to_csv('/opt/airflow/data/customers.csv', index=False)

# Definir los argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2️⃣ Define el DAG
with DAG(
    'customer_etl_pipeline',
    default_args=default_args,  # Argumentos por defecto
    description='ETL pipeline to fetch customers, upload to GCS, and load to BigQuery',
    schedule=timedelta(days=1),  # Ejecutar una vez al día
    catchup=False,
    tags=['etl', 'gcs', 'bigquery']
) as dag:


    # 2. Fetch
    fetch_task = PythonOperator(
        task_id='fetch_customers',
        python_callable=fetch_customers
    )

    # 3. Upload to GCS
    upload_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/opt/airflow/data/customers.csv',
        dst='customers/customers.csv',
        bucket=bucket,
        gcp_conn_id='google_cloud_default'
    )

    # 4. Load to BigQuery SIN SOBREESCRIBIR (append)
    bq_load_task = BigQueryInsertJobOperator(
        task_id='load_to_bq',
        configuration={
            "load": {
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id
                },
                "sourceUris": [f"gs://{bucket}/customers/customers.csv"],
                "sourceFormat": "CSV",
                "autodetect": True,
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_APPEND"
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    # ✅ Dependencias
    fetch_task >> upload_task >> bq_load_task
