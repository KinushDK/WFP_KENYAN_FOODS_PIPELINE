SELECT
    county,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    ROUND(AVG(price)::numeric, 2) AS avg_price
FROM {{ ref('stg_food_prices') }}
GROUP BY 
    county,
    EXTRACT(YEAR FROM date),
    EXTRACT(MONTH FROM date)