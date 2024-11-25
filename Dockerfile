# Используем официальный образ Airflow
FROM apache/airflow:2.5.3

# Устанавливаем рабочую директорию
WORKDIR /opt/airflow

# Устанавливаем необходимые пакеты под root-пользователем
#USER root
#RUN apt-get update && \
#    apt-get install -y --no-install-recommends \
#    build-essential \
#    libpq-dev \
#    && apt-get autoremove -y \
#    && apt-get clean \
#    && rm -rf /var/lib/apt/lists/*

## Переключаемся на пользователя airflow для установки Python-зависимостей
#USER airflow

# Копируем requirements.txt и устанавливаем Python-зависимости
COPY requirements.txt /opt/airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Копируем весь исходный код и конфигурацию
COPY . /opt/airflow

# Команда для запуска Airflow
CMD ["airflow", "standalone"]