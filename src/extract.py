import os

from environs import Env

env = Env()
env.read_env("../.env")
username = env.str("KAGGLE_USERNAME", None)
key = env.str("KAGGLE_KEY", None)
import kaggle


def download_dataset():
    # Инициализация API Kaggle
    api = kaggle.KaggleApi()
    api.authenticate()

    # Путь к датасету на Kaggle
    dataset_path = 'antonkozyriev/game-recommendations-on-steam'
    download_path = '/tmp/kaggle_dataset'

    # Создание директории для загрузки, если она не существует
    os.makedirs(download_path, exist_ok=True)

    # Загрузка датасета
    api.dataset_download_files(dataset_path, path=download_path, unzip=True)