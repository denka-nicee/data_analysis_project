FROM apache/airflow:2.10.1

# Копируем requirements.txt в контейнер
COPY requirements.txt /opt/airflow/requirements.txt

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Копируем все остальные файлы проекта (dags, defs и т.д.)
COPY dags /opt/airflow/dags
COPY defs /opt/airflow/defs
