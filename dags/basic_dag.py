import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    dag_id='basic_example_dag',
    description='Un DAG de ejemplo con tareas A, B, C y D',
    default_args=default_args,
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',  # ← cambia esto
    catchup=False,
    tags=['ejemplo'],
) as dag:

    task_a = EmptyOperator(task_id='A')
    task_b = EmptyOperator(task_id='B')
    task_c = EmptyOperator(task_id='C')
    task_d = EmptyOperator(task_id='D')

    task_a >> [task_b, task_c] >> task_d
