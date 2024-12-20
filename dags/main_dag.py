import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from defs.calculate_correlation import calculate_correlation_for_hours
from defs.download import download_dataset
from defs.load_json import load_json
from defs.transform import process_and_load_games, process_and_load_recommendations, process_and_load_users, \
    load_data_to_postgres_using_copy, load_users_to_postgres, load_recommendations_to_postgres

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
    # Определяем пути к файлам
    games_path = "/tmp/kaggle_dataset/games.csv"
    recommendations_path = "/tmp/kaggle_dataset/recommendations.csv"
    users_path = "/tmp/kaggle_dataset/users.csv"

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

        app_ids = process_and_load_games(games_path)
        # app_ids = load_data_to_postgres_using_copy(games_path)


        logger.info(f"Данные об играх обработаны")

        # Обработка данных о пользователях
        logger.info("Начало предобработки данных о пользователях")
        # process_and_load_users(users_path)
        load_users_to_postgres(users_path)
        logger.info(f"Данные о пользователях обработаны")

        # Обработка данных о рекомендациях
        logger.info("Начало предобработки данных о рекомендациях")
        # process_and_load_recommendations(recommendations_path, app_ids)
        load_recommendations_to_postgres(recommendations_path)
        logger.info(f"Данные о рекомендациях обработаны")


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
    template_searchpath='/opt/airflow/'
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
# Задача для выполнения первого SQL-скрипта
process_metadata_task = PythonOperator(
    task_id='process_metadata',
    python_callable=load_json,
    dag=dag,
)
move_dds_to_stg = PostgresOperator(
    task_id='create_tables',
    sql="sql_scripts/move_dds_to_stg.sql",
    postgres_conn_id='dataset_db',
    dag=dag,
)

average_hours_dm = PostgresOperator(
    task_id='create_average_hours_dm',
    sql="sql_scripts/average_hours_dds_to_dm.sql",
    postgres_conn_id='dataset_db',
    dag=dag,
)

price_review_summary_dm = PostgresOperator(
    task_id='create_price_review_dm',
    sql='sql_scripts/price_review_summery_dds_to_dm.sql',
    postgres_conn_id='dataset_db',
    dag=dag,
)

get_correlation_for_hours = PythonOperator(
    task_id='get_correlation_for_hours',
    python_callable=calculate_correlation_for_hours,
    dag=dag,
)

tags_summary_dm = PostgresOperator(
    task_id='create_tags_summary_dm',
    sql='sql_scripts/H3_tags_summary.sql',
    postgres_conn_id='dataset_db',
    dag=dag,
)

# Установка порядка выполнения задач
download_task >> [process_metadata_task, process_data_task] >> move_dds_to_stg >> [average_hours_dm,
                                                                                   price_review_summary_dm,
                                                                                   tags_summary_dm] >> get_correlation_for_hours