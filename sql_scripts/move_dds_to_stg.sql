-- Создание схемы dds, если она не существует
DROP SCHEMA IF EXISTS public;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'dds') THEN
        CREATE SCHEMA dds;
    END IF;
END;
$$;

-- Удаляем существующую таблицу, если она существует
DROP TABLE IF EXISTS dds.users CASCADE;

DROP TABLE IF EXISTS dds.games CASCADE;

DROP TABLE IF EXISTS dds.recommendations CASCADE;

DROP TABLE IF EXISTS dds.metadata CASCADE;

-- Создаем новую таблицу с типами данных VARCHAR для рейтинга и соотношения положительных отзывов
CREATE TABLE dds.games (
    app_id INT,
    title VARCHAR(255),
    date_release DATE,
    rating VARCHAR(255),  -- Тип данных изменен на VARCHAR
    positive_ratio INT,  -- Тип данных изменен на INT
    user_reviews INT,
    price_final DECIMAL
);

-- Создаем таблицу dds.recommendations
CREATE TABLE dds.recommendations (
    app_id INT,
    helpful INT,
    funny INT,
    date VARCHAR(255),
    is_recommended VARCHAR(255),
    hours INT,
    user_id INT,
    review_id INT
);

-- Вставляем данные в новую таблицу с учетом преобразования типов
INSERT INTO dds.games (app_id, title, date_release, rating, positive_ratio, user_reviews, price_final)
SELECT
    app_id,
    title,
    date_release::date AS date_release,  -- Преобразуем timestamp в DATE
    rating,
    positive_ratio,
    user_reviews,
    price_final
FROM dds_stg.games;

-- Создаем новую таблицу с типами данных VARCHAR для поля `is_recommended`
INSERT INTO dds.recommendations (app_id, helpful, funny, date, is_recommended, hours, user_id, review_id)
SELECT
    app_id,
    helpful,
    funny,
    date,
    is_recommended,
    hours,
    user_id,
    review_id
FROM dds_stg.recommendations;

-- Создаем новую таблицу с типами данных VARCHAR для полей
CREATE TABLE dds.users (
    user_id INT,
    products INT,
    reviews INT
);

-- Вставляем данные в новую таблицу
INSERT INTO dds.users (user_id, products, reviews)
SELECT
    user_id,
    products,
    reviews
FROM dds_stg.users;

-- Создаем таблицу metadata в схеме dds, очищая данные от игр с пустыми tags
CREATE TABLE dds.metadata AS
SELECT
    app_id,
    tags
FROM
    dds_stg.metadata
WHERE
    cardinality(tags) > 0
;


