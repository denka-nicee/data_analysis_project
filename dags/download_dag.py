from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import download_dataset

# Определение DAGа
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
}

dag = DAG(
    'download_kaggle_dataset',
    default_args=default_args,
    description='A DAG to download multiple files (CSV and JSON) from Kaggle',
    schedule_interval=None,  # Запуск только по триггеру
    catchup=False,  # Не запускать прошлые даты
)

# Определение задачи
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

# Установка порядка выполнения задач
download_task
