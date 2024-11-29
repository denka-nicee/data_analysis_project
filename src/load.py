import logging
import pandas as pd
from sqlalchemy import create_engine

# Настройка логирования
logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Параметры подключения PostgreSQL
DATABASE_URL = "postgresql://big_data_user:yhgurjfnvdx@postgres:5432/big_data_db"


# Функция для создания аналитической таблицы
def create_and_load_database():
    engine = create_engine(DATABASE_URL)
    query = """
    CREATE DATABASE cleaned_data;
    """
    try:
        # Выполнение запроса и получение данных
        df = pd.read_sql(query, engine)
        # logger.info(f"Данные успешно загружены: {df.head()}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
    finally:
        engine.dispose()