from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Variables de entorno
bucket = os.getenv("GCS_BUCKET_NAME")
project_id = os.getenv("BIGQUERY_PROJECT_ID")
dataset_id = os.getenv("BIGQUERY_DATASET_ID")
table_id = os.getenv("BIGQUERY_TABLE_ID")

file_path = '/opt/airflow/data/customers.csv'

# 1️⃣ Obtener datos y agregar al CSV local
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
    df_new = pd.DataFrame(records)
    os.makedirs('/opt/airflow/data', exist_ok=True)

    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.drop_duplicates(subset=["email"], keep="last", inplace=True)
    df_combined.to_csv(file_path, index=False)

# 2️⃣ Crear la tabla si no existe
def check_and_create_bq_table():
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)
        print(f"✅ Table {table_id} already exists.")
    except NotFound:
        schema = [
            bigquery.SchemaField("full_name", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("national_id", "STRING"),
            bigquery.SchemaField("country", "STRING")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"📦 Created table {table_id}.")

# Argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG principal
with DAG(
    'customer_etl_pipeline',
    default_args=default_args,
    description='ETL: fetch -> GCS -> BQ (append + dedupe)',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['etl', 'gcs', 'bigquery']
) as dag:

    # 1. Fetch data
    fetch_task = PythonOperator(
        task_id='fetch_customers',
        python_callable=fetch_customers
    )

    # 2. Check/Create table
    check_create_table_task = PythonOperator(
        task_id='check_and_create_bq_table',
        python_callable=check_and_create_bq_table
    )

    # 3. Upload CSV to GCS
    upload_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=file_path,
        dst='customers/customers.csv',
        bucket=bucket,
        gcp_conn_id='google_cloud_default'
    )

    # 4. Load GCS CSV to BigQuery
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

    # 5. Eliminar duplicados en BigQuery
    dedupe_task = BigQueryInsertJobOperator(
        task_id='remove_duplicates_bq',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
                    SELECT AS VALUE *
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY email ORDER BY CURRENT_TIMESTAMP()) AS row_num
                        FROM `{project_id}.{dataset_id}.{table_id}`
                    )
                    WHERE row_num = 1
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    # DAG dependencies
    fetch_task >> check_create_table_task >> upload_task >> bq_load_task >> dedupe_task
