-- Удаляем таблицу, если она уже существует
DROP TABLE IF EXISTS dm.tags_summary;

-- Создаём новую таблицу для сводной аналитики по тегам
CREATE TABLE dm.tags_summary (
    tag TEXT,
    game_count INT,
    avg_hours DOUBLE PRECISION
);

-- Вставляем данные в таблицу
INSERT INTO dm.tags_summary (tag, game_count, avg_hours)
SELECT
    tag,
    COUNT(DISTINCT m.app_id) AS game_count,
    AVG(r.hours) AS avg_hours
FROM dds.metadata m
CROSS JOIN LATERAL unnest(m.tags) AS tag
LEFT JOIN dds.recommendations r ON m.app_id = r.app_id
GROUP BY tag
ORDER BY game_count ASC;