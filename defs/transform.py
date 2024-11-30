import logging

import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

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
            conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            logger.info(f"Таблица {table_name} удалена (если она существовала).")
    except SQLAlchemyError as e:
        logger.error(f"Ошибка при удалении таблицы {table_name}: {e}")


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


# Обработка и загрузка данных о играх
def process_and_load_games(path, postgres_conn_id='dataset_db', chunk_size=10000):
    # Получаем параметры подключения из Airflow
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"

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

            # Загрузка данных в базу данных
            chunk.to_sql("games", engine, if_exists='append', index=False)
            logger.info(f"Загружено {len(chunk)} строк в таблицу games")

    except Exception as e:
        logger.error(f"Ошибка при обработке и загрузке данных о играх: {e}")
        raise

    return app_ids


# Обработка и загрузка данных о рекомендациях
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
            chunk.to_sql("recommendations", engine, if_exists='append', index=False)
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
            chunk.to_sql("users", engine, if_exists='append', index=False)
            logger.info(f"Загружено {len(chunk)} строк в таблицу users")

    except Exception as e:
        logger.error(f"Ошибка при обработке и загрузке данных о пользователях: {e}")
        raise
