-- Удаление таблицы, если она уже существует
DROP TABLE IF EXISTS dm.price_review_summary;

-- Создание новой таблицы в схеме dm
CREATE TABLE dm.price_review_summary (
    price_category VARCHAR(50),
    game_count INT,
    total_reviews_count INT,
    positive_reviews_count INT
);

-- Заполнение таблицы данными
WITH price_categories AS (
    SELECT
        app_id,
        CASE
            WHEN price_final BETWEEN 1 AND 2 THEN 'a'
            WHEN price_final BETWEEN 2 AND 3 THEN 'b'
            WHEN price_final BETWEEN 3 AND 4 THEN 'c'
            WHEN price_final BETWEEN 4 AND 6 THEN 'd'
	        WHEN price_final BETWEEN 6 AND 10 THEN 'e'
			WHEN price_final BETWEEN 10 AND 15 THEN 'f'
			WHEN price_final BETWEEN 15 AND 20 THEN 'g'
			WHEN price_final BETWEEN 20 AND 25 THEN 'h'
			WHEN price_final BETWEEN 25 AND 30 THEN 'i'
			WHEN price_final BETWEEN 30 AND 35 THEN 'j'
			WHEN price_final BETWEEN 35 AND 40 THEN 'k'
			WHEN price_final BETWEEN 41 AND 45 THEN 'l'
			WHEN price_final BETWEEN 46 AND 50 THEN 'm'
			WHEN price_final BETWEEN 51 AND 55 THEN 'n'
			WHEN price_final BETWEEN 56 AND 60 THEN 'o'
			WHEN price_final BETWEEN 61 AND 65 THEN 'p'
			WHEN price_final BETWEEN 66 AND 70 THEN 'q'
			WHEN price_final BETWEEN 71 AND 75 THEN 'r'
			WHEN price_final BETWEEN 76 AND 80 THEN 's'
			WHEN price_final BETWEEN 81 AND 85 THEN 't'
			WHEN price_final BETWEEN 86 AND 90 THEN 'u'
			WHEN price_final BETWEEN 91 AND 95 THEN 'v'
			WHEN price_final BETWEEN 96 AND 100 THEN 'w'
			WHEN price_final BETWEEN 101 AND 105 THEN 'x'
			WHEN price_final BETWEEN 106 AND 110 THEN 'y'
			WHEN price_final BETWEEN 111 AND 300 THEN 'z'
		END AS price_category
    FROM dds.games
),
reviews_aggregated AS (
    SELECT
        r.app_id,
        COUNT(*) AS total_reviews,
        SUM(CASE WHEN LOWER(r.is_recommended) = 'true' THEN 1 ELSE 0 END) AS positive_reviews
    FROM dds.recommendations r
    GROUP BY r.app_id
)
INSERT INTO dm.price_review_summary (price_category, game_count, total_reviews_count, positive_reviews_count)
SELECT
    p.price_category,
    COUNT(DISTINCT g.app_id) AS game_count,
    SUM(r.total_reviews) AS total_reviews_count,
    SUM(r.positive_reviews) AS positive_reviews_count
FROM price_categories p
JOIN dds.games g ON p.app_id = g.app_id
JOIN reviews_aggregated r ON g.app_id = r.app_id
GROUP BY p.price_category
ORDER BY p.price_category;








