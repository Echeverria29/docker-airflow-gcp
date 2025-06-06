from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os
import requests
import pandas as pd

# Variables de entorno
bucket = os.getenv("GCS_BUCKET_NAME")
project_id = os.getenv("BIGQUERY_PROJECT_ID")
dataset_id = os.getenv("BIGQUERY_DATASET_ID")
table_id = os.getenv("BIGQUERY_TABLE_ID")
gcp_conn_id = 'google_cloud_default'

def fetch_customers(**context):
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

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f'customers_{timestamp}.csv'
    local_path = f'/opt/airflow/data/{filename}'

    os.makedirs('/opt/airflow/data', exist_ok=True)
    df.drop_duplicates(subset=["email"], inplace=True)
    df.to_csv(local_path, index=False)

    context['ti'].xcom_push(key='csv_filename', value=filename)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'customer_etl_pipeline_v2',
    default_args=default_args,
    description='ETL: fetch -> GCS -> BQ (dedup antes de insertar)',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['etl', 'gcs', 'bigquery']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_customers',
        python_callable=fetch_customers
    )

    def upload_to_gcs(**context):
        filename = context['ti'].xcom_pull(key='csv_filename', task_ids='fetch_customers')
        local_path = f'/opt/airflow/data/{filename}'
        destination_path = f'customers/{filename}'
        context['ti'].xcom_push(key='gcs_path', value=destination_path)

        return LocalFilesystemToGCSOperator(
            task_id='upload_to_gcs_internal',
            src=local_path,
            dst=destination_path,
            bucket=bucket,
            gcp_conn_id=gcp_conn_id
        ).execute(context=context)

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs
    )

    check_create_table_task = BigQueryCreateEmptyTableOperator(
        task_id='check_and_create_bq_table',
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=[
            {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "national_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country", "type": "STRING", "mode": "NULLABLE"}
        ],
        exists_ok=True,
        gcp_conn_id=gcp_conn_id
    )

    def load_to_bigquery(**context):
        gcs_path = context['ti'].xcom_pull(key='gcs_path', task_ids='upload_to_gcs')
        uri = f'gs://{bucket}/{gcs_path}'

        return BigQueryInsertJobOperator(
            task_id='bq_load_internal',
            configuration={
                "load": {
                    "destinationTable": {
                        "projectId": project_id,
                        "datasetId": dataset_id,
                        "tableId": table_id
                    },
                    "sourceUris": [uri],
                    "sourceFormat": "CSV",
                    "autodetect": False,
                    "skipLeadingRows": 1,
                    "writeDisposition": "WRITE_APPEND"
                }
            },
            gcp_conn_id=gcp_conn_id
        ).execute(context=context)

    bq_load_task = PythonOperator(
        task_id='load_to_bq',
        python_callable=load_to_bigquery
    )

    dedupe_task = BigQueryInsertJobOperator(
        task_id='remove_duplicates_bq',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
                    SELECT full_name, email, national_id, country
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


    # ✅ ORDEN corregido:
    fetch_task >> upload_task >> check_create_table_task >> bq_load_task >> dedupe_task
