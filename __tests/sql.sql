WITH Search AS (
    SELECT
        id,
        (SELECT `AVG`(`daily_prices`) AS `avg` FROM `$.rates`) AS rates
    FROM
        `$.data.hotels`
),
MongoDB AS (
    SELECT
        MONGO('connection', 'Hotels', (SELECT `id` AS id FROM Search)) AS documents
),
MongoDB2 AS (
    SELECT
        MONGO('connection', 'Reviews', (SELECT `id` AS id FROM Search)) AS documents
)
SELECT
    `Q1.id` AS id,
    `AVG`(`Q1.rates.{?}.avg`) AS avg_rates,
    `Q2.kind` AS kind,
    `Q2.name` AS name,
    `Q2.description_struct.{0}.paragraphs.{0}` AS `description`,
    `Q2.images.{0}` AS thumbnail,
    `Q2.star_rating.$numberInt` AS stars,
    `Q2.serp_filters` AS filters,
    `Q3.rating.$numberDouble` AS rate,
    `COUNT`(`Q3.reviews`) AS reviews
FROM
    Search Q1
    JOIN `MongoDB.documents` Q2 ON `Q1.id` = `Q2.code`
    LEFT JOIN `MongoDB2.documents` Q3 ON `Q1.id` = `Q3.name`