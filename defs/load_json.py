import logging

import os
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def load_json():
    # Определяем пути к файлам
    json_path = '/tmp/kaggle_dataset/games_metadata.json'

    try:
        # Проверка существования файлов
        for file_path in [json_path]:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                raise FileNotFoundError(f"Файл не найден: {file_path}")
            if os.path.getsize(file_path) == 0:
                logger.error(f"Файл пуст: {file_path}")
                raise ValueError(f"Файл пуст: {file_path}")

        # Обработка данных о играх
        logger.info("Начало предобработки данных о играх")

        # Чтение и загрузка данных в PostgreSQL
        process_and_load_metadata(json_path)

        logger.info("Данные об играх обработаны и загружены в базу данных")

    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        raise


def read_json_data(path):
    try:
        chunk = pd.read_json(path, lines=True)  # Используйте lines=True для чтения построчно
        return chunk
    except Exception as e:
        logger.error(f"Ошибка при чтении JSON-файла {path}: {e}")
        raise


def create_schema_if_not_exists(engine):
    with engine.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS dds_stg;")
        logger.info("Схема 'dds_stg' успешно создана или уже существует.")


def drop_table_if_exists(engine, table_name):
    with engine.connect() as conn:
        conn.execute(f"DROP TABLE IF EXISTS dds_stg.{table_name};")
        logger.info(f"Таблица {table_name} успешно удалена.")


def load_data_to_postgres(chunk, engine, table_name):
    try:
        chunk.to_sql(table_name, engine, schema='dds_stg', if_exists='append', index=False)
        logger.info(f"Загружено {len(chunk)} строк в таблицу {table_name}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в таблицу {table_name}: {e}")
        raise


def process_and_load_metadata(path, postgres_conn_id='dataset_db'):
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"
    engine = create_engine(db_url)

    try:
        create_schema_if_not_exists(engine)
        drop_table_if_exists(engine, "metadata")

        # Чтение данных из JSON
        chunk = read_json_data(path)

        # Загрузка данных в таблицу metadata
        load_data_to_postgres(chunk, engine, "metadata")

    except Exception as e:
        logger.error(f"Ошибка при обработке и загрузке данных о метаданных: {e}")
        raise

