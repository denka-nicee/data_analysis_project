CREATE SCHEMA IF NOT EXISTS dm;
CREATE SCHEMA IF NOT EXISTS idw;

-- Создаем или заменяем представление с расчетом avg_hours_per_game
CREATE OR REPLACE VIEW idw.avg_hours_per_game AS
SELECT
    app_id,
    AVG(hours) AS avg_hours
FROM
    dds_stg.recommendations
GROUP BY
    app_id;

DROP TABLE IF EXISTS dm.average_hours_hypothesis CASCADE;

-- Создаем таблицу average_hours_hypothesis с расчетом среднего avg_hours для каждого positive_ratio
CREATE TABLE IF NOT EXISTS dm.average_hours_hypothesis AS
SELECT
    CAST(g.positive_ratio AS INT) AS positive_ratio, -- Преобразуем в тип INT
    AVG(COALESCE(a.avg_hours, 0)) AS avg_hours -- Среднее avg_hours для positive_ratio
FROM
    dds_stg.games g
LEFT JOIN
    idw.avg_hours_per_game a
ON
    g.app_id = a.app_id
GROUP BY
    CAST(g.positive_ratio AS INT); -- Группируем по positive_ratio

