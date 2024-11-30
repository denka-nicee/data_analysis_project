import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from defs.download import download_dataset
from defs.transform import load_and_preprocess_games, load_and_preprocess_recommendations, load_and_preprocess_users

# Настройка логирования
logger = logging.getLogger(__name__)


# Определение функции проверки наличия файлов
def is_dataset_exists(dataset_path) -> bool:
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
    if is_dataset_exists("/tmp/kaggle_dataset/games.csv"):
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
    if is_dataset_exists("/tmp/kaggle_dataset/cleaned_data/clear_games.csv"):
        return
    else:
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

            games_df, app_ids = load_and_preprocess_games(games_path)

            logger.info(f"Количество игр после предобработки: {len(games_df)}")

            # Обработка данных о рекомендациях
            logger.info("Начало предобработки данных о рекомендациях")
            recommendations_df = load_and_preprocess_recommendations(recommendations_path, app_ids)
            logger.info(f"Количество рекомендаций после предобработки: {len(recommendations_df)}")

            # Обработка данных о пользователях
            logger.info("Начало предобработки данных о пользователях")
            users_df = load_and_preprocess_users(users_path)
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
    'big_data_project',
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
