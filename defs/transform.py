import logging

import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

import psycopg2
from psycopg2 import sql
import pandas as pd
from airflow.hooks.base_hook import BaseHook
import os

# Настройка логирования
logger = logging.getLogger(__name__)

# Обработчики типов данных для каждой таблицы
types_games = {
    'app_id': int, 'title': object, 'date_release': 'datetime64[ns]', 'win': bool, 'mac': bool, 'linux': bool,
    'rating': object, 'positive_ratio': int, 'user_reviews': int, 'price_final': float,
    'price_original': float, 'discount': float, 'steam_deck': bool
}

types_recommendations = {
    'app_id': int, 'helpful': int, 'funny': int, 'date': 'datetime64[ns]',
    'is_recommended': bool, 'hours': float, 'user_id': int, 'review_id': int
}

types_users = {
    'user_id': int, 'products': int, 'reviews': int
}


# Удаление таблицы, если она уже существует
def drop_table_if_exists(engine, table_name):
    try:
        with engine.connect() as conn:
            conn.execute(f"DROP TABLE IF EXISTS dds_stg.{table_name} CASCADE")
            logger.info(f"Таблица dds_stg.{table_name} удалена (если она существовала).")
    except SQLAlchemyError as e:
        logger.error(f"Ошибка при удалении таблицы dds_stg.{table_name}: {e}")


def common_transform(chunk, table_name):
    # Удаляем строки с пустыми значениями
    chunk = chunk.dropna()

    # Удаляем дубликаты
    chunk = chunk.drop_duplicates()

    if table_name == 'games':
        types = types_games
    elif table_name == 'recommendations':
        types = types_recommendations
    elif table_name == 'users':
        types = types_users

    # Приводим типы данных
    for key, value in types.items():
        if value in [int, float]:
            chunk[key] = pd.to_numeric(chunk[key], errors='coerce')
        elif value == 'datetime64[ns]':
            chunk[key] = pd.to_datetime(chunk[key], errors='coerce')
        elif value == bool:
            chunk[key] = chunk[key].astype('bool', errors='ignore')

    # Удаляем строки с пустыми значениями после приведения типов
    chunk = chunk.dropna()

    return chunk


def load_data_to_postgres_using_copy(path, postgres_conn_id='dataset_db'):
    # Получаем параметры подключения из Airflow
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"

    # Устанавливаем соединение с базой данных через sqlalchemy
    engine = create_engine(db_url)

    try:
        # Проверка существования файла
        if not os.path.exists(path):
            raise FileNotFoundError(f"Файл не найден: {path}")
        if os.path.getsize(path) == 0:
            raise ValueError(f"Файл пуст: {path}")

        # Удаляем таблицу, если она существует
        drop_table_if_exists(engine, 'games')

        # Чтение данных из CSV с помощью pandas
        df = pd.read_csv(path, encoding='ISO-8859-1')

        # Убедимся, что DataFrame имеет правильные типы данных
        # Преобразование типов в соответствии с заданными
        types_games = {
            'app_id': 'int', 'title': 'str', 'date_release': 'datetime64[ns]', 'win': 'bool', 'mac': 'bool',
            'linux': 'bool', 'rating': 'str', 'positive_ratio': 'int', 'user_reviews': 'int', 'price_final': 'float64',
            'price_original': 'float64', 'discount': 'float64', 'steam_deck': 'bool'
        }

        # Преобразуем типы данных DataFrame согласно заданным типам
        df = df.astype(types_games)

        # Загружаем данные в базу
        df.to_sql('games', engine, schema='dds_stg', if_exists='replace', index=False)

        print(f"Данные из файла {path} успешно загружены в таблицу dds_stg.games")

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
    finally:
        # Закрываем соединение
        engine.dispose()


def load_recommendations_to_postgres(path, postgres_conn_id='dataset_db', chunksize=10000):
    import tempfile

    # Получаем параметры подключения из Airflow
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"

    # Устанавливаем соединение с базой данных
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    try:
        # Проверка существования файла
        if not os.path.exists(path):
            raise FileNotFoundError(f"Файл не найден: {path}")
        if os.path.getsize(path) == 0:
            raise ValueError(f"Файл пуст: {path}")

        # Удаление таблицы перед загрузкой данных
        table_name = 'recommendations'
        try:
            cur.execute(f"DROP TABLE IF EXISTS dds_stg.{table_name} CASCADE")
            conn.commit()
            print(f"Таблица dds_stg.{table_name} успешно удалена (если существовала).")
        except Exception as e:
            print(f"Ошибка при удалении таблицы dds_stg.{table_name}: {e}")
            conn.rollback()
            raise

        # Создание таблицы (пример, необходимо настроить в зависимости от структуры данных)
        cur.execute("""
            CREATE TABLE dds_stg.recommendations (
                app_id INT,
                helpful INT,
                funny INT,
                date TIMESTAMP,
                is_recommended BOOLEAN,
                hours FLOAT,
                user_id INT,
                review_id INT
            )
        """)
        conn.commit()
        print(f"Таблица dds_stg.{table_name} успешно создана.")

        # Определяем типы данных
        types_recommendations = {
            'app_id': int, 'helpful': int, 'funny': int, 'date': 'str',
            'is_recommended': bool, 'hours': float, 'user_id': int, 'review_id': int
        }

        # Чтение файла чанками
        chunk_iter = pd.read_csv(path, encoding='ISO-8859-1', chunksize=chunksize)

        for i, chunk in enumerate(chunk_iter, start=1):
            print(f"Обработка чанка {i}...")

            # Приводим типы данных в текущем чанке
            chunk = chunk.astype(types_recommendations)

            # Преобразуем дату в формат, подходящий для PostgreSQL
            chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

            # Сохраняем чанк во временный CSV-файл
            with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv') as temp_file:
                chunk.to_csv(temp_file.name, index=False, header=False)
                temp_file_path = temp_file.name

            # Используем команду COPY для загрузки данных из временного файла
            with open(temp_file_path, 'r', encoding='utf-8') as f:
                copy_sql = f"COPY dds_stg.{table_name} FROM STDIN WITH CSV DELIMITER ','"
                cur.copy_expert(copy_sql, f)
                conn.commit()

            # Удаляем временный файл
            os.remove(temp_file_path)

        print(f"Данные из файла {path} успешно загружены в таблицу dds_stg.{table_name}")

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        conn.rollback()
    finally:
        # Закрываем соединение
        cur.close()
        conn.close()


def load_users_to_postgres(path, postgres_conn_id='dataset_db', chunksize=20000):
    import tempfile

    # Получаем параметры подключения из Airflow
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"

    # Устанавливаем соединение с базой данных
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    try:
        # Проверка существования файла
        if not os.path.exists(path):
            raise FileNotFoundError(f"Файл не найден: {path}")
        if os.path.getsize(path) == 0:
            raise ValueError(f"Файл пуст: {path}")

        # Удаление таблицы перед загрузкой данных
        table_name = 'users'
        try:
            cur.execute(f"DROP TABLE IF EXISTS dds_stg.{table_name} CASCADE")
            conn.commit()
            print(f"Таблица dds_stg.{table_name} успешно удалена (если существовала).")
        except Exception as e:
            print(f"Ошибка при удалении таблицы dds_stg.{table_name}: {e}")
            conn.rollback()
            raise

        # Создание таблицы (пример, необходимо настроить в зависимости от структуры данных)
        cur.execute("""
            CREATE TABLE dds_stg.users (
                user_id INT,
                products INT,
                reviews INT
            )
        """)
        conn.commit()
        print(f"Таблица dds_stg.{table_name} успешно создана.")

        # Определяем типы данных
        types_users = {
            'user_id': int, 'products': int, 'reviews': int
        }

        # Чтение файла чанками
        chunk_iter = pd.read_csv(path, encoding='ISO-8859-1', chunksize=chunksize)

        for i, chunk in enumerate(chunk_iter, start=1):
            print(f"Обработка чанка {i}...")

            # Приводим типы данных в текущем чанке
            chunk = chunk.astype(types_users)

            # Сохраняем чанк во временный CSV-файл
            with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv') as temp_file:
                chunk.to_csv(temp_file.name, index=False, header=False)
                temp_file_path = temp_file.name

            # Используем команду COPY для загрузки данных из временного файла
            with open(temp_file_path, 'r', encoding='utf-8') as f:
                copy_sql = f"COPY dds_stg.{table_name} FROM STDIN WITH CSV DELIMITER ','"
                cur.copy_expert(copy_sql, f)
                conn.commit()

            # Удаляем временный файл
            os.remove(temp_file_path)

        print(f"Данные из файла {path} успешно загружены в таблицу dds_stg.{table_name}")

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        conn.rollback()
    finally:
        # Закрываем соединение
        cur.close()
        conn.close()


## Обработка и загрузка данных о рекомендациях
def process_and_load_recommendations(path, app_ids, postgres_conn_id='dataset_db', chunk_size=10000):
    # Получаем параметры подключения из Airflow
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"

    # Создаем соединение с базой данных
    engine = create_engine(db_url)

    try:

        # Удаляем таблицу, если она существует
        drop_table_if_exists(engine, "recommendations")

        # Чтение данных из CSV по частям
        for chunk in pd.read_csv(path, encoding='ISO-8859-1', chunksize=chunk_size):
            # Удаляем рекомендации для удалённых игр
            chunk = chunk[chunk['app_id'].isin(app_ids)]

            common_transform(chunk, "recommendations")

            # Загрузка данных в базу данных
            chunk.to_sql("recommendations", engine, schema='dds_stg', if_exists='append', index=False)
            logger.info(f"Загружено {len(chunk)} строк в таблицу recommendations")

    except Exception as e:
        logger.error(f"Ошибка при обработке и загрузке данных о рекомендациях: {e}")
        raise


# Обработка и загрузка данных о пользователях
def process_and_load_users(path, postgres_conn_id='dataset_db', chunk_size=10000):
    # Получаем параметры подключения из Airflow
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"

    # Создаем соединение с базой данных
    engine = create_engine(db_url)

    try:

        # Удаляем таблицу, если она существует
        drop_table_if_exists(engine, "users")

        # Чтение данных из CSV по частям
        for chunk in pd.read_csv(path, encoding='ISO-8859-1', chunksize=chunk_size):
            common_transform(chunk, "users")

            # Загрузка данных в базу данных
            chunk.to_sql("users", engine, schema='dds_stg', if_exists='append', index=False)
            logger.info(f"Загружено {len(chunk)} строк в таблицу users")

    except Exception as e:
        logger.error(f"Ошибка при обработке и загрузке данных о пользователях: {e}")
        raise


# Обработка и загрузка данных о играх
def process_and_load_games(path, postgres_conn_id='dataset_db', chunk_size=10000):
    # Получаем параметры подключения из Airflow
    conn_id = BaseHook.get_connection(postgres_conn_id)
    # conn_id.schema = 'dds'
    # print(conn_id.schema)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}?options=-csearch_path=dds_stg"

    # Создаем соединение с базой данных
    engine = create_engine(db_url)

    app_ids = set()

    try:
        # Удаляем таблицу, если она существует
        drop_table_if_exists(engine, "games")

        # Чтение данных из CSV по частям
        for chunk in pd.read_csv(path, encoding='ISO-8859-1', chunksize=chunk_size):
            # Удаляем игры с "DLC" или "Soundtrack" в названии
            chunk = chunk[~chunk['title'].str.contains("DLC|Soundtrack", case=False, na=False)]

            common_transform(chunk, "games")

            # Добавление app_id в сет
            app_ids.update(chunk['app_id'].unique())

            # Создаём схему, если она ещё не существует
            with engine.connect() as conn:
                conn.execute("CREATE SCHEMA IF NOT EXISTS dds_stg;")
                logger.info("Схема 'dds_stg' успешно создана или уже существует.")

            # Загрузка данных в базу данных
            chunk.to_sql("games", engine, schema='dds_stg', if_exists='append', index=False)
            logger.info(f"Загружено {len(chunk)} строк в таблицу games")

    except Exception as e:
        logger.error(f"Ошибка при обработке и загрузке данных о играх: {e}")
        raise

    return app_ids
