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
def is_dataset_exists() -> bool:
    dataset_path = "/tmp/kaggle_dataset/games.csv"

    if not os.path.exists(dataset_path):
        logger.info(f"Файл не существует: {dataset_path}")
        return False

    if os.path.getsize(dataset_path) > 0:  # Проверяем, что файл не пустой
        logger.info(f"Файл существует и не пуст: {dataset_path}")
        return True

    logger.info(f"Файл существует, но пуст: {dataset_path}")
    return False


# Определение функции для загрузки данных
def download_data():
    if is_dataset_exists():
        return
    else:
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
        for file_path in [games_path, recommendations_path, users_path]:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                raise FileNotFoundError(f"Файл не найден: {file_path}")
            if os.path.getsize(file_path) == 0:
                logger.error(f"Файл пуст: {file_path}")
                raise ValueError(f"Файл пуст: {file_path}")

        # Обработка данных о играх
        logger.info("Начало предобработки данных о играх")
        games_df = pd.read_csv(games_path, encoding='ISO-8859-1')

        # Удаляем игры с "DLC" или "Soundtrack" в названии
        games_df = games_df[~games_df['title'].str.contains("DLC|Soundtrack", case=False, na=False, regex=True)]

        # Удаляем строки с пустыми значениями, дубликаты и приводим типы данных
        games_df = games_df.dropna().drop_duplicates()
        for key, value in types_games.items():
            if value in [int, float]:
                games_df[key] = pd.to_numeric(games_df[key], errors='coerce')
            elif value == 'datetime64[ns]':
                games_df[key] = pd.to_datetime(games_df[key], errors='coerce')
            elif value == bool:
                games_df[key] = games_df[key].astype('bool', errors='ignore')
        games_df = games_df.dropna()

        deleted_app_ids = games_df['app_id'].unique()
        logger.info(f"Количество игр после предобработки: {len(games_df)}")

        # Обработка данных о рекомендациях
        logger.info("Начало предобработки данных о рекомендациях")
        recommendations_df = pd.read_csv(recommendations_path, encoding='ISO-8859-1')
        recommendations_df = recommendations_df[~recommendations_df['app_id'].isin(deleted_app_ids)]
        recommendations_df = recommendations_df.dropna().drop_duplicates()
        for key, value in types_recommendations.items():
            if value in [int, float]:
                recommendations_df[key] = pd.to_numeric(recommendations_df[key], errors='coerce')
            elif value == 'datetime64[ns]':
                recommendations_df[key] = pd.to_datetime(recommendations_df[key], errors='coerce')
            elif value == bool:
                recommendations_df[key] = recommendations_df[key].astype('bool', errors='ignore')
        recommendations_df = recommendations_df.dropna()
        logger.info(f"Количество рекомендаций после предобработки: {len(recommendations_df)}")

        # Обработка данных о пользователях
        logger.info("Начало предобработки данных о пользователях")
        users_df = pd.read_csv(users_path, encoding='ISO-8859-1')
        users_df = users_df.dropna().drop_duplicates()
        for key, value in types_users.items():
            if value in [int, float]:
                users_df[key] = pd.to_numeric(users_df[key], errors='coerce')
            elif value == 'datetime64[ns]':
                users_df[key] = pd.to_datetime(users_df[key], errors='coerce')
            elif value == bool:
                users_df[key] = users_df[key].astype('bool', errors='ignore')
        users_df = users_df.dropna()
        logger.info(f"Количество пользователей после предобработки: {len(users_df)}")

        # Сохранение очищенных данных в новые CSV файлы
        logger.info("Сохранение очищенных данных о играх")
        games_df.to_csv(f'{output_path}/clear_games.csv', encoding='ISO-8859-1', sep=',', header=True, index=False)

        logger.info("Сохранение очищенных данных о рекомендациях")
        recommendations_df.to_csv(f'{output_path}/clear_recommendations.csv', encoding='ISO-8859-1', sep=',',
                                  header=True, index=False)

        logger.info("Сохранение очищенных данных о пользователях")
        users_df.to_csv(f'{output_path}/clear_users.csv', sep=',', header=True, index=False)

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
    'kaggle_dataset',
    default_args=default_args,
    schedule_interval=None,  # Запуск только по триггеру
    catchup=False,  # Не запускать прошлые даты
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
download_task >> process_data_task
