-- noinspection SqlNoDataSourceInspectionForFile

-- noinspection SqlDialectInspectionForFile

CREATE SCHEMA IF NOT EXISTS dm;

CREATE OR REPLACE VIEW dds_stg.avg_hours_per_game AS
SELECT
    app_id,
    AVG(hours) AS avg_hours
FROM
    dds_stg.recommendations
GROUP BY
    app_id;

CREATE TABLE IF NOT EXISTS dm.game_summary AS
SELECT
    g.app_id,
    CAST(g.positive_ratio AS INT) AS positive_ratio, -- Преобразуем в тип INT
    COALESCE(a.avg_hours, 0) AS avg_hours
FROM
    dds_stg.games g
LEFT JOIN
    dds_stg.avg_hours_per_game a
ON
    g.app_id = a.app_id;
