import json
import logging
import os

import numpy as np
import pandas as pd
import psycopg2
from airflow.hooks.base_hook import BaseHook
from psycopg2 import sql
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def read_json_data(path):
    try:
        chunk = pd.read_json(path, lines=True)  # Используем lines=True для чтения построчно
        return chunk
    except Exception as e:
        logger.error(f"Ошибка при чтении JSON-файла {path}: {e}")
        raise


def create_schema_if_not_exists(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS dds_stg;")
            conn.commit()
            logger.info("Схема 'dds_stg' успешно создана или уже существует.")
    except Exception as e:
        logger.error(f"Ошибка при создании схемы: {e}")
        conn.rollback()
        raise


def drop_table_if_exists(conn, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS dds_stg.{table}").format(
                table=sql.Identifier(table_name)
            ))
            conn.commit()
            logger.info(f"Таблица {table_name} успешно удалена.")
    except Exception as e:
        logger.error(f"Ошибка при удалении таблицы {table_name}: {e}")
        conn.rollback()
        raise


def create_table(conn, table_name):
    try:
        with conn.cursor() as cur:
            create_table_query = sql.SQL("""
                CREATE TABLE dds_stg.{table} (
                    app_id BIGINT PRIMARY KEY,
                    tags TEXT[]
                )
            """).format(table=sql.Identifier(table_name))
            cur.execute(create_table_query)
            conn.commit()
            logger.info(f"Таблица {table_name} успешно создана.")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы {table_name}: {e}")
        conn.rollback()
        raise


def load_data_to_postgres_psycopg(df, conn, table_name):
    try:
        # Преобразуем все столбцы в стандартные типы Python
        # Это помогает избежать проблем с адаптацией типов psycopg2
        df = df.convert_dtypes()

        # Преобразуем 'app_id' в стандартный тип Python int
        if pd.api.types.is_integer_dtype(df['app_id']):
            df['app_id'] = df['app_id'].astype(int)
        else:
            logger.warning("'app_id' не является целочисленным типом. Попытка преобразовать в int.")
            df['app_id'] = df['app_id'].apply(lambda x: int(x) if not pd.isnull(x) else None)

        # Убедимся, что 'tags' является списком строк
        df['tags'] = df['tags'].apply(lambda x: x if isinstance(x, list) else [])

        # Подготовка данных в виде списка кортежей
        records = df.to_dict(orient='records')
        data = [(record['app_id'], record['tags']) for record in records]

        # Определение SQL-запроса для массовой вставки
        insert_query = sql.SQL("""
            INSERT INTO dds_stg.{table} (app_id, tags)
            VALUES %s
            ON CONFLICT (app_id) DO NOTHING
        """).format(table=sql.Identifier(table_name))

        # Выполнение массовой вставки с использованием execute_values
        with conn.cursor() as cur:
            execute_values(cur, insert_query.as_string(conn), data, page_size=1000)
            conn.commit()
            logger.info(f"Загружено {len(data)} строк в таблицу {table_name}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в таблицу {table_name}: {e}")
        conn.rollback()
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
                    # Убедимся, что 'tags' является списком
                    tags = data['tags'] if isinstance(data['tags'], list) else []
                    # Убедимся, что 'app_id' является целым числом
                    app_id = data['app_id']
                    if isinstance(app_id, (int, np.integer)):
                        app_id = int(app_id)
                    else:
                        try:
                            app_id = int(app_id)
                        except ValueError:
                            logger.warning(f"Не удалось преобразовать 'app_id' {app_id} в int. Пропускаем запись.")
                            continue
                    records.append({'app_id': app_id, 'tags': tags})
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

        # Проверка типов данных
        logger.info(f"Типы данных в DataFrame:\n{df.dtypes}")

        # Загрузка данных в PostgreSQL
        postgres_conn_id = 'dataset_db'
        process_and_load_to_postgres(df, postgres_conn_id, table_name="metadata")

        logger.info("Данные об играх обработаны и загружены в базу данных")

    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        raise


def process_and_load_to_postgres(df, postgres_conn_id, table_name="metadata"):
    """
    Загружает DataFrame в PostgreSQL с помощью psycopg2.
    """
    conn_details = BaseHook.get_connection(postgres_conn_id)
    conn_string = f"host={conn_details.host} port={conn_details.port} dbname={conn_details.schema} user={conn_details.login} password={conn_details.password}"

    try:
        # Устанавливаем соединение с базой данных
        with psycopg2.connect(conn_string) as conn:
            # Создание схемы, если не существует
            create_schema_if_not_exists(conn)

            # Удаление старой таблицы, если она существует
            drop_table_if_exists(conn, table_name)

            # Создание таблицы
            create_table(conn, table_name)

            # Загрузка данных
            load_data_to_postgres_psycopg(df, conn, table_name)

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в PostgreSQL: {e}")
        raise
