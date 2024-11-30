import psycopg2
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine


# Функция для загрузки данных из CSV в PostgreSQL
def load_csv_to_postgres(csv_file_path, table_name, postgres_conn_id='dataset_db'):
    # Получаем параметры подключения из Airflow
    conn_id = BaseHook.get_connection(postgres_conn_id)
    db_url = f"postgresql://{conn_id.login}:{conn_id.password}@{conn_id.host}:{conn_id.port}/{conn_id.schema}"

    # Создаем соединение с базой данных
    engine = create_engine(db_url)

    try:
        # Чтение данных из CSV по частям (chunks)
        chunk_size = 10000  # Размер чанка
        for chunk in pd.read_csv(csv_file_path, sep=',', encoding='ISO-8859-1', chunksize=chunk_size):
            # Загрузка данных в таблицу PostgreSQL по частям
            chunk.to_sql(table_name, engine, if_exists='append', index=False)

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        raise
