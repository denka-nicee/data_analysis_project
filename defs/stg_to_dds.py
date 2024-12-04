# Функция для миграции данных
def migrate_all_data():
    # Подключение к исходной базе данных
    engine_src = create_engine('postgresql://username:password@localhost/source_db')
    # Подключение к целевой базе данных
    engine_dest = create_engine('postgresql://username:password@localhost/destination_db')

    # Соединения с базами данных
    with engine_src.connect() as src_connection, engine_dest.connect() as dest_connection:
        try:
            # Получение данных из исходной базы
            result = src_connection.execute("SELECT * FROM source_table")

            # Перенос данных в целевую базу
            for row in result:
                dest_connection.execute(
                    "INSERT INTO destination_table (column1, column2) VALUES (?, ?)",
                    (row['column1'], row['column2'])
                )
        except Exception as e:
            print(f"Ошибка миграции данных: {e}")
        finally:
            # Закрытие соединений (закрытие соединений через 'with' происходит автоматически)
            pass
