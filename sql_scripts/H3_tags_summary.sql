-- Начало транзакции для обеспечения целостности данных
BEGIN;

-- 1. Удаляем таблицу dm.tags_summary, если она уже существует
DROP TABLE IF EXISTS dm.tags_summary;

-- 2. Создаём новую таблицу dm.tags_summary для сводной аналитики по тегам
CREATE TABLE dm.tags_summary (
    tag TEXT,
    game_count INT,
    avg_hours DOUBLE PRECISION
);

-- 3. Создаём необходимые индексы для оптимизации запросов

-- 3.1. Индексы на таблице dds.metadata
CREATE INDEX IF NOT EXISTS idx_metadata_app_id ON dds.metadata(app_id);

-- 3.2. Индексы на таблице dds.recommendations
CREATE INDEX IF NOT EXISTS idx_recommendations_app_id ON dds.recommendations(app_id);

-- 4. Создаём промежуточную таблицу для тегов, если она ещё не существует
--    Это позволяет избежать повторного выполнения UNNEST при каждом запуске запроса
DROP TABLE IF EXISTS dds.metadata_tags;

CREATE TABLE dds.metadata_tags AS
SELECT
    m.app_id,
    tag
FROM dds.metadata m
CROSS JOIN LATERAL unnest(m.tags) AS tag;

-- 5. Создаём индексы на промежуточной таблице dds.metadata_tags
CREATE INDEX IF NOT EXISTS idx_metadata_tags_tag ON dds.metadata_tags(tag);
CREATE INDEX IF NOT EXISTS idx_metadata_tags_app_id ON dds.metadata_tags(app_id);

-- 6. Создаём промежуточную таблицу для агрегированных рекомендаций
--    Это уменьшает объём данных при JOIN и ускоряет выполнение запроса
DROP TABLE IF EXISTS dds.aggregated_recommendations;

CREATE TABLE dds.aggregated_recommendations AS
SELECT
    app_id,
    AVG(hours) AS avg_hours
FROM dds.recommendations
GROUP BY app_id;

-- 7. Создаём индекс на агрегированной таблице рекомендаций
CREATE INDEX IF NOT EXISTS idx_aggregated_recommendations_app_id
    ON dds.aggregated_recommendations(app_id);

-- 8. Вставляем агрегированные данные в dm.tags_summary без ORDER BY для повышения скорости
INSERT INTO dm.tags_summary (tag, game_count, avg_hours)
SELECT
    t.tag,
    COUNT(t.app_id) AS game_count,
    AVG(ar.avg_hours) AS avg_hours
FROM dds.metadata_tags t
LEFT JOIN dds.aggregated_recommendations ar
    ON t.app_id = ar.app_id
GROUP BY t.tag;
COMMIT;

-- (Опционально) Очистка промежуточных таблиц, если они больше не нужны
 DROP TABLE IF EXISTS dds.metadata_tags;
 DROP TABLE IF EXISTS dds.aggregated_recommendations;