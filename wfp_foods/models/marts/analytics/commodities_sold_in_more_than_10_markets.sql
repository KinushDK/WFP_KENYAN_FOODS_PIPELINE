SELECT
    commodity,
    COUNT(DISTINCT market) AS market_count
FROM {{ ref('stg_food_prices') }}
GROUP BY commodity
HAVING COUNT(DISTINCT market) > 10
ORDER BY market_count DESC