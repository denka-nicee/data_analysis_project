-- noinspection SqlDialectInspectionForFile

-- noinspection SqlNoDataSourceInspectionForFile

-- Создание схемы dds, если она не существует

DROP TABLE IF EXISTS public;

CREATE SCHEMA IF NOT EXISTS dds;


-- Удаляем существующую таблицу, если она существует
DROP TABLE IF EXISTS dds.users CASCADE;

DROP TABLE IF EXISTS dds.games CASCADE;

DROP TABLE IF EXISTS dds.recommendations CASCADE;



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

-- Вставляем данные в новую таблицу с учетом преобразования типов
INSERT INTO dds.games (app_id, title, date_release, rating, positive_ratio, user_reviews, price_final)
SELECT
    app_id,
    title,
    TO_DATE(date_release, 'YYYY-MM-DD') AS date_release,  -- Преобразуем строку в тип DATE
    rating,  -- Оставляем как есть (тип VARCHAR)
    positive_ratio,  -- Оставляем как есть (тип VARCHAR)
    CAST(user_reviews AS INT) AS user_reviews,             -- Преобразуем в INT, если нужно
    CAST(price_final AS DECIMAL) AS price_final             -- Преобразуем в DECIMAL, если нужно
FROM dds_stg.games;




-- Создаем новую таблицу с типами данных VARCHAR для поля `is_recommended`
CREATE TABLE dds.recommendations (
    app_id INT,
    helpful INT,
    funny INT,
    date DATE,  -- Оставляем тип DATE для поля `date`
    is_recommended VARCHAR(255),  -- Тип данных изменен на VARCHAR
    hours INT,
    user_id INT,
    review_id INT
);

-- Вставляем данные в новую таблицу с учетом преобразования типов
INSERT INTO dds.recommendations (app_id, helpful, funny, date, is_recommended, hours, user_id, review_id)
SELECT
    app_id,
    helpful,
    funny,
    CASE
        WHEN date IS NULL THEN NULL
        ELSE TO_DATE(date, 'YYYY-MM-DD')  -- Преобразуем строку в формат DATE
    END AS date,
    is_recommended,  -- Оставляем как есть (тип VARCHAR)
    hours,
    user_id,
    review_id
FROM dds_stg.recommendations;


-- Удаляем существующую таблицу, если она существует

-- Создаем новую таблицу с типами данных VARCHAR для полей
CREATE TABLE dds.users (
    user_id INT,
    products VARCHAR(255),  -- Тип данных для products изменен на VARCHAR
    reviews VARCHAR(255)   -- Тип данных для reviews изменен на VARCHAR
);

-- Вставляем данные в новую таблицу
INSERT INTO dds.users (user_id, products, reviews)
SELECT
    user_id,
    products,
    reviews
FROM dds_stg.users;

