SELECT
    commodity,
    admin2 AS county,
    market,
    price::numeric AS price,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month
FROM wfp_food_prices_clean