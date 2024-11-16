import os

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile
from environs import Env

env = Env()
env.read_env("../.env")
username = env.str("KAGGLE_USERNAME", None)
key = env.str("KAGGLE_KEY", None)
import kaggle


def download_dataset():
    # Инициализация API Kaggle
    api = kaggle.KaggleApi()
    api.authenticate()

    # Путь к датасету на Kaggle
    dataset_path = 'antonkozyriev/game-recommendations-on-steam'
    download_path = '/tmp/kaggle_dataset'

    # Создание директории для загрузки, если она не существует
    os.makedirs(download_path, exist_ok=True)

    # Загрузка датасета
    api.dataset_download_files(dataset_path, path=download_path, unzip=True)

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
