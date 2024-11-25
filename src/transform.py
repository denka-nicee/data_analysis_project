import pandas as pd


# Функции для предобработки данных

def load_and_preprocess_games(path, types):
    """
    Загружает и предобрабатывает данные о играх.
    """
    df = pd.read_csv(path, encoding='ISO-8859-1')

    # Удаляем игры с "DLC" или "Soundtrack" в названии
    df = df[~df['title'].str.contains("DLC|Soundtrack", case=False, na=False)]

    # Удаляем строки с пустыми значениями
    df = df.dropna()

    # Удаляем дубликаты
    df = df.drop_duplicates()

    # Приводим типы данных
    for key, value in types.items():
        if value in [int, float]:
            df[key] = pd.to_numeric(df[key], errors='coerce')
        elif value == 'datetime64[ns]':
            df[key] = pd.to_datetime(df[key], errors='coerce')
        elif value == bool:
            df[key] = df[key].astype('bool', errors='ignore')

    # Удаляем строки с пустыми значениями после приведения типов
    df = df.dropna()

    return df


def load_and_preprocess_recommendations(path, types, deleted_app_ids):
    """
    Загружает и предобрабатывает данные о рекомендациях.
    """
    df = pd.read_csv(path, encoding='ISO-8859-1')

    # Удаляем рекомендации для удалённых игр
    df = df[~df['app_id'].isin(deleted_app_ids)]

    # Удаляем строки с пустыми значениями
    df = df.dropna()

    # Удаляем дубликаты
    df = df.drop_duplicates()

    # Приводим типы данных
    for key, value in types.items():
        if value in [int, float]:
            df[key] = pd.to_numeric(df[key], errors='coerce')
        elif value == 'datetime64[ns]':
            df[key] = pd.to_datetime(df[key], errors='coerce')
        elif value == bool:
            df[key] = df[key].astype('bool', errors='ignore')

    # Удаляем строки с пустыми значениями после приведения типов
    df = df.dropna()

    return df


def load_and_preprocess_users(path, types):
    """
    Загружает и предобрабатывает данные о пользователях.
    """
    df = pd.read_csv(path, encoding='ISO-8859-1')

    # Удаляем строки с пустыми значениями
    df = df.dropna()

    # Удаляем дубликаты
    df = df.drop_duplicates()

    # Приводим типы данных
    for key, value in types.items():
        if value in [int, float]:
            df[key] = pd.to_numeric(df[key], errors='coerce')
        elif value == 'datetime64[ns]':
            df[key] = pd.to_datetime(df[key], errors='coerce')
        elif value == bool:
            df[key] = df[key].astype('bool', errors='ignore')

    # Удаляем строки с пустыми значениями после приведения типов
    df = df.dropna()

    return df


# Определяем ожидаемые типы данных для каждого столбца
types_games = {
    'app_id': int, 'title': object, 'date_release': 'datetime64[ns]', 'win': bool, 'mac': bool, 'linux': bool,
    'rating': object, 'positive_ratio': int, 'user_reviews': int, 'price_final': float,
    'price_original': float, 'discount': float, 'steam_deck': bool  # Исправлено: убрали лишнюю точку с запятой
}

types_recommendations = {
    'app_id': int, 'helpful': int, 'funny': int, 'date': 'datetime64[ns]',
    'is_recommended': bool, 'hours': float, 'user_id': int, 'review_id': int
}

types_users = {
    'user_id': int, 'products': int, 'reviews': int
}