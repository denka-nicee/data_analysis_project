import pandas as pd

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


# Функции для предобработки данных
def load_and_preprocess_games(path):
    """
    Загружает и предобрабатывает данные о играх.
    """
    games_df = pd.read_csv(path, encoding='ISO-8859-1')

    # Удаляем игры с "DLC" или "Soundtrack" в названии
    games_df = games_df[~games_df['title'].str.contains("DLC|Soundtrack", case=False, na=False)]

    # Удаляем строки с пустыми значениями
    games_df = games_df.dropna()

    # Удаляем дубликаты
    games_df = games_df.drop_duplicates()

    # Приводим типы данных
    for key, value in types_games.items():
        if value in [int, float]:
            games_df[key] = pd.to_numeric(games_df[key], errors='coerce')
        elif value == 'datetime64[ns]':
            games_df[key] = pd.to_datetime(games_df[key], errors='coerce')
        elif value == bool:
            games_df[key] = games_df[key].astype('bool', errors='ignore')

    # Удаляем строки с пустыми значениями после приведения типов
    games_df = games_df.dropna()

    app_ids = games_df['app_id'].unique()
    return games_df, app_ids


def load_and_preprocess_recommendations(path, app_ids):
    """
    Загружает и предобрабатывает данные о рекомендациях.
    """
    recommendations_df = pd.read_csv(path, encoding='ISO-8859-1')

    # Удаляем строки с пустыми значениями
    recommendations_df = recommendations_df.dropna()

    # Удаляем дубликаты
    recommendations_df = recommendations_df.drop_duplicates()

    # Удаляем рекомендации для удалённых игр
    recommendations_df = recommendations_df[recommendations_df['app_id'].isin(app_ids)]


    # Приводим типы данных
    for key, value in types_recommendations.items():
        if value in [int, float]:
            recommendations_df[key] = pd.to_numeric(recommendations_df[key], errors='coerce')
        elif value == 'datetime64[ns]':
            recommendations_df[key] = pd.to_datetime(recommendations_df[key], errors='coerce')
        elif value == bool:
            recommendations_df[key] = recommendations_df[key].astype('bool', errors='ignore')

    # Удаляем строки с пустыми значениями после приведения типов
    recommendations_df = recommendations_df.dropna()

    return recommendations_df


def load_and_preprocess_users(path):
    """
    Загружает и предобрабатывает данные о пользователях.
    """
    users_df = pd.read_csv(path, encoding='ISO-8859-1')

    # Удаляем строки с пустыми значениями
    users_df = users_df.dropna()

    # Удаляем дубликаты
    users_df = users_df.drop_duplicates()

    # Приводим типы данных
    for key, value in types_users.items():
        if value in [int, float]:
            users_df[key] = pd.to_numeric(users_df[key], errors='coerce')
        elif value == 'datetime64[ns]':
            users_df[key] = pd.to_datetime(users_df[key], errors='coerce')
        elif value == bool:
            users_df[key] = users_df[key].astype('bool', errors='ignore')

    # Удаляем строки с пустыми значениями после приведения типов
    users_df = users_df.dropna()

    return users_df
