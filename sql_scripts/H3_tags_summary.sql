-- Удаляем таблицу, если она уже существует
DROP TABLE IF EXISTS dm.tags_summary;

-- Создаём новую таблицу для сводной аналитики по тегам
CREATE TABLE dm.tags_summary (
    tag TEXT,
    game_count INT
);

-- Вставляем данные в таблицу
INSERT INTO dm.tags_summary (tag, game_count)
SELECT
    tag,
    COUNT(DISTINCT m.app_id) AS game_count
FROM dds.metadata m
CROSS JOIN LATERAL unnest(m.tags) AS tag
GROUP BY tag
ORDER BY game_count ASC;