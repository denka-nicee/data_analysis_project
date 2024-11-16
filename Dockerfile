# Используем официальный образ Airflow
FROM apache/airflow:2.5.3

# Устанавливаем необходимые пакеты под root-пользователем
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Переключаемся на пользователя airflow для установки Python-зависимостей
USER airflow

# Копируем requirements.txt и устанавливаем Python-зависимости
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Устанавливаем environs для работы с переменными окружения
RUN pip install --no-cache-dir environs

# Копируем весь исходный код и конфигурацию
COPY . /opt/airflow

# Устанавливаем рабочую директорию
WORKDIR /opt/airflow

# Команда для запуска Airflow
CMD ["airflow", "standalone"]