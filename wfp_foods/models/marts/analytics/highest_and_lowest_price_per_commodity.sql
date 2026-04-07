SELECT
    commodity,
    MIN(price) AS lowest_price,
    MAX(price) AS highest_price
FROM {{ ref('stg_food_prices') }}
GROUP BY commodity
ORDER BY commodity