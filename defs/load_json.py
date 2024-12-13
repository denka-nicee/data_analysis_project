import json
import logging
import os

import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Text
from sqlalchemy.dialects.postgresql import ARRAY

logger = logging.getLogger(__name__)


def read_json_data(path):
    try:
        chunk = pd.read_json(path, lines=True)  # Используем lines=True для чтения построчно
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
        chunk.to_sql(
            table_name,
            engine,
            schema='dds_stg',
            if_exists='append',
            index=False,
            dtype={'tags': ARRAY(Text)}
        )
        logger.info(f"Загружено {len(chunk)} строк в таблицу {table_name}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в таблицу {table_name}: {e}")
        raise


def read_and_process_json(path):
    """
    Читает JSON и возвращает DataFrame со всеми тегами без фильтрации.
    """
    try:
        records = []
        with open(path, 'r', encoding='utf-8') as file:
            for line in file:
                data = json.loads(line)
                if 'app_id' in data and 'tags' in data:
                    records.append({'app_id': data['app_id'], 'tags': data['tags']})
        df = pd.DataFrame(records)
        return df
    except Exception as e:
        logger.error(f"Ошибка при обработке JSON: {e}")
        raise


def load_json():
    # Определяем путь к JSON-файлу
    json_path = '/tmp/kaggle_dataset/games_metadata.json'

    try:
        # Проверка существования файла
        if not os.path.exists(json_path):
            logger.error(f"Файл не найден: {json_path}")
            raise FileNotFoundError(f"Файл не найден: {json_path}")
        if os.path.getsize(json_path) == 0:
            logger.error(f"Файл пуст: {json_path}")
            raise ValueError(f"Файл пуст: {json_path}")

        # Чтение и предобработка данных игр
        logger.info("Начало предобработки данных о играх")
        df = read_and_process_json(json_path)

        # Загрузка данных в PostgreSQL
        postgres_conn_id = 'dataset_db'
        process_and_load_to_postgres(df, postgres_conn_id, table_name="metadata")

        logger.info("Данные об играх обработаны и загружены в базу данных")

    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        raise


def process_and_load_to_postgres(df, postgres_conn_id, table_name="metadata"):
    """
    Загружает DataFrame в PostgreSQL.
    """
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"
    engine = create_engine(db_url)

    try:
        # Создание схемы и удаление старой таблицы
        create_schema_if_not_exists(engine)
        drop_table_if_exists(engine, table_name)

        # Загрузка данных
        load_data_to_postgres(df, engine, table_name)

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в PostgreSQL: {e}")
        raise
    finally:
        engine.dispose()
