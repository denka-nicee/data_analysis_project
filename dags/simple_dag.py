# import os
# from datetime import datetime
#
# import pandas as pd
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from environs import Env
#
# env = Env()
# env.read_env("../.env")
# username = env.str("KAGGLE_USERNAME", None)
# key = env.str("KAGGLE_KEY", None)
#
# Env.str("KAGGLE_USERNAME", None)
# Env.str("KAGGLE_KEY", None)
# from kaggle.api.kaggle_api_extended import KaggleApi
#
# def download_dataset():
#     # Инициализация API Kaggle
#     api = KaggleApi()
#     api.authenticate()
#
#     # Загрузка датасета
#     dataset_path = 'path/to/dataset'  # Замените на путь к вашему датасету на Kaggle
#     download_path = '/tmp/kaggle_dataset'
#     api.dataset_download_files(dataset_path, path=download_path, unzip=True)
#
#     # Чтение CSV файла
#     csv_file = f'{download_path}/{os.listdir(download_path)[0]}'  # Предполагается, что в архиве только один CSV файл
#     df = pd.read_csv(csv_file)
#
#     # Вывод первых 10 строк
#     print(df.head(10))
#
# # Определение DAGа
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 10, 1),
#     'retries': 1,
# }
#
# dag = DAG(
#     'download_kaggle_dataset',
#     default_args=default_args,
#     description='A simple DAG to download a dataset from Kaggle and print the first 10 rows',
#     schedule_interval=None,
# )
#
# # Определение задачи
# download_task = PythonOperator(
#     task_id='download_dataset',
#     python_callable=download_dataset,
#     dag=dag,
# )
#
# # Установка порядка выполнения задач
# download_task