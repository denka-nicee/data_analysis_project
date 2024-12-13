import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
import math


def calculate_correlation_for_hours():
    # Получаем параметры подключения
    conn_id = 'dataset_db'  # conn_id, который используется в Airflow
    conn = BaseHook.get_connection(conn_id)
    db_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

    # Создаем соединение с базой данных через SQLAlchemy
    engine = create_engine(db_url)

    # SQL-запрос для извлечения данных
    query = """
    SELECT
        positive_ratio,
        avg_hours
    FROM
        dm.average_hours_hypothesis
    """

    # Чтение данных в DataFrame
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)

    print(df[:5])
    # Рассчитываем коэффициент корреляции Пирсона

    x = pd.to_numeric(df['positive_ratio'], errors='coerce')
    y = pd.to_numeric(df['avg_hours'], errors='coerce')

    # Определяем ранги
    x_rank = x.rank()
    y_rank = y.rank()

    # Рассчитываем разности рангов и их квадраты
    d = x_rank - y_rank
    d_squared = d ** 2

    # Коэффициент корреляции Спирмена
    n = len(x)
    spearman_corr = 1 - (6 * d_squared.sum()) / (n * (n ** 2 - 1))
    print(spearman_corr)
