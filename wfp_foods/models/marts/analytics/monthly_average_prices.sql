SELECT
    year,
    month,
    ROUND(AVG(price)::numeric, 2) AS avg_price
FROM {{ ref('stg_food_prices') }}
GROUP BY year, month
ORDER BY year, month