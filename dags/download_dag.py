from datetime import datetime, timedelta
import os
import logging
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from src.extract import download_dataset
from src.transform import (
    types_games,
    types_recommendations,
    types_users
)

# Настройка логирования
logger = logging.getLogger(__name__)


# Определение функции проверки наличия файлов
def check_dataset_exists():
    dataset_path = "/tmp/kaggle_dataset/games.csv"
    if not os.path.exists(dataset_path):
        logger.info(f"Директория не существует: {dataset_path}")
        return "download_dataset"
    files = os.listdir(dataset_path)
    if len(files) > 0:
        logger.info(f"Файлы уже существуют в директории: {dataset_path}")
        return "process_data"
    logger.info(f"Директория пуста: {dataset_path}")
    return "download_dataset"


# Определение функции для загрузки данных
def download_data():
    logger.info("Начало загрузки данных")
    try:
        download_dataset()
        logger.info("Загрузка данных завершена")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise


# Определение функции для предобработки данных
def process_data():
    # Определяем пути к файлам
    games_path = "/tmp/kaggle_dataset/games.csv"
    recommendations_path = "/tmp/kaggle_dataset/recommendations.csv"
    users_path = "/tmp/kaggle_dataset/users.csv"

    # Определяем путь для сохранения очищенных данных
    output_path = "/tmp/kaggle_dataset/cleaned_data"

    # Создаем директорию, если она не существует
    os.makedirs(output_path, exist_ok=True)

    try:
        # Проверка существования файлов
        if not os.path.exists(games_path):
            logger.error(f"Файл не найден: {games_path}")
            raise FileNotFoundError(f"Файл не найден: {games_path}")
        if not os.path.exists(recommendations_path):
            logger.error(f"Файл не найден: {recommendations_path}")
            raise FileNotFoundError(f"Файл не найден: {recommendations_path}")
        if not os.path.exists(users_path):
            logger.error(f"Файл не найден: {users_path}")
            raise FileNotFoundError(f"Файл не найден: {users_path}")

        # Проверка размера файлов
        if os.path.getsize(games_path) == 0:
            logger.error(f"Файл пуст: {games_path}")
            raise ValueError(f"Файл пуст: {games_path}")
        if os.path.getsize(recommendations_path) == 0:
            logger.error(f"Файл пуст: {recommendations_path}")
            raise ValueError(f"Файл пуст: {recommendations_path}")
        if os.path.getsize(users_path) == 0:
            logger.error(f"Файл пуст: {users_path}")
            raise ValueError(f"Файл пуст: {users_path}")

        # Загрузка и предобработка данных о играх с использованием итераторов
        logger.info("Начало предобработки данных о играх")
        chunk_size = 10000  # Размер фрагмента
        games_chunks = pd.read_csv(games_path, encoding='ISO-8859-1', chunksize=chunk_size)
        processed_games_chunks = []
        for chunk in games_chunks:
            # Удаляем игры с "DLC" или "Soundtrack" в названии
            chunk = chunk[~chunk['title'].str.contains("DLC|Soundtrack", case=False, na=False, regex=True)]

            # Удаляем строки с пустыми значениями
            chunk = chunk.dropna()

            # Удаляем дубликаты
            chunk = chunk.drop_duplicates()

            # Приводим типы данных
            for key, value in types_games.items():
                if value in [int, float]:
                    chunk[key] = pd.to_numeric(chunk[key], errors='coerce')
                elif value == 'datetime64[ns]':
                    chunk[key] = pd.to_datetime(chunk[key], errors='coerce')
                elif value == bool:
                    chunk[key] = chunk[key].astype('bool', errors='ignore')

            # Удаляем строки с пустыми значениями после приведения типов
            chunk = chunk.dropna()

            processed_games_chunks.append(chunk)

        # Объединяем все фрагменты в один DataFrame
        res_games = pd.concat(processed_games_chunks, ignore_index=True)
        deleted_app_ids = res_games['app_id'].unique()  # Получаем уникальные app_id после удаления DLC и Soundtrack
        logger.info(f"Количество игр после предобработки: {len(res_games)}")

        # Загрузка и предобработка данных о рекомендациях с использованием итераторов
        logger.info("Начало предобработки данных о рекомендациях")
        recommendations_chunks = pd.read_csv(recommendations_path, encoding='ISO-8859-1', chunksize=chunk_size)
        processed_recommendations_chunks = []
        for chunk in recommendations_chunks:
            # Удаляем рекомендации для удалённых игр
            chunk = chunk[~chunk['app_id'].isin(deleted_app_ids)]

            # Удаляем строки с пустыми значениями
            chunk = chunk.dropna()

            # Удаляем дубликаты
            chunk = chunk.drop_duplicates()

            # Приводим типы данных
            for key, value in types_recommendations.items():
                if value in [int, float]:
                    chunk[key] = pd.to_numeric(chunk[key], errors='coerce')
                elif value == 'datetime64[ns]':
                    chunk[key] = pd.to_datetime(chunk[key], errors='coerce')
                elif value == bool:
                    chunk[key] = chunk[key].astype('bool', errors='ignore')

            # Удаляем строки с пустыми значениями после приведения типов
            chunk = chunk.dropna()

            processed_recommendations_chunks.append(chunk)

        # Объединяем все фрагменты в один DataFrame
        res_recommendations = pd.concat(processed_recommendations_chunks, ignore_index=True)
        logger.info(f"Количество рекомендаций после предобработки: {len(res_recommendations)}")

        # Загрузка и предобработка данных о пользователях с использованием итераторов
        logger.info("Начало предобработки данных о пользователях")
        users_chunks = pd.read_csv(users_path, encoding='ISO-8859-1', chunksize=chunk_size)
        processed_users_chunks = []
        for chunk in users_chunks:
            # Удаляем строки с пустыми значениями
            chunk = chunk.dropna()

            # Удаляем дубликаты
            chunk = chunk.drop_duplicates()

            # Приводим типы данных
            for key, value in types_users.items():
                if value in [int, float]:
                    chunk[key] = pd.to_numeric(chunk[key], errors='coerce')
                elif value == 'datetime64[ns]':
                    chunk[key] = pd.to_datetime(chunk[key], errors='coerce')
                elif value == bool:
                    chunk[key] = chunk[key].astype('bool', errors='ignore')

            # Удаляем строки с пустыми значениями после приведения типов
            chunk = chunk.dropna()

            processed_users_chunks.append(chunk)

        # Объединяем все фрагменты в один DataFrame
        res_users = pd.concat(processed_users_chunks, ignore_index=True)
        logger.info(f"Количество пользователей после предобработки: {len(res_users)}")

        # Сохранение очищенных данных в новые CSV файлы
        logger.info("Сохранение очищенных данных о играх")
        res_games.to_csv(f'{output_path}/clear_games.csv', encoding='ISO-8859-1', sep=',', header=True, index=False)

        logger.info("Сохранение очищенных данных о рекомендациях")
        res_recommendations.to_csv(f'{output_path}/clear_recommendations.csv', encoding='ISO-8859-1', sep=',',
                                   header=True, index=False)

        logger.info("Сохранение очищенных данных о пользователях")
        res_users.to_csv(f'{output_path}/clear_users.csv', sep=',', header=True, index=False)

        logger.info("Данные успешно обработаны и сохранены")
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        raise


# Определение DAGа
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
}

dag = DAG(
    'download_kaggle_dataset',
    default_args=default_args,
    description='A DAG to download multiple files (CSV and JSON) from Kaggle and process them',
    schedule_interval=None,  # Запуск только по триггеру
    catchup=False,  # Не запускать прошлые даты
)

# Определение задачи ветвления
branch = BranchPythonOperator(
    task_id='branch',
    python_callable=check_dataset_exists,
    dag=dag,
)

# Определение задачи загрузки данных
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_data,
    dag=dag,
)

# Определение задачи предобработки данных
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

# Установка порядка выполнения задач
branch >> [download_task, process_data_task]
download_task >> process_data_task