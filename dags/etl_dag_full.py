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

    # Guardar el nombre del archivo para las siguientes tareas
    context['ti'].xcom_push(key='csv_filename', value=filename)

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

    check_create_table_task = PythonOperator(
        task_id='check_and_create_bq_table',
        python_callable=check_and_create_bq_table
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
            gcp_conn_id='google_cloud_default'
        ).execute(context=context)

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs
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
                    "autodetect": True,
                    "skipLeadingRows": 1,
                    "writeDisposition": "WRITE_APPEND"
                }
            },
            gcp_conn_id='google_cloud_default'
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
                    SELECT AS VALUE *
                    FROM (
                        SELECT * EXCEPT(row_num),
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

    # Definir las dependencias entre las tareas
    fetch_task >> check_create_table_task >> upload_task >> bq_load_task >> dedupe_task
