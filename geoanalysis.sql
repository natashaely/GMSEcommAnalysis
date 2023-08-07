-- Seasonality Analysis
-- Breaking down sales data on a monthly basis

SELECT
  EXTRACT(MONTH
  FROM
    PARSE_DATE('%Y%m%d', event_date)) AS sale_month,
  COUNT(DISTINCT user_pseudo_id) AS unique_users,
  SUM(item.quantity) AS total_products_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  sale_month
ORDER BY
  sale_month;


-- Breaking down sales data on a weekly basis

SELECT
  EXTRACT(WEEK
  FROM
    PARSE_DATE('%Y%m%d', event_date)) AS sale_week,
  COUNT(DISTINCT user_pseudo_id) AS unique_users,
  SUM(item.quantity) AS total_products_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  sale_week
ORDER BY
  sale_week;


-- Geographic Analysis
-- Storing geographic information by country 

SELECT
  geo.country,
  COUNT(DISTINCT user_pseudo_id) AS unique_users,
  SUM(item.quantity) AS total_products_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  geo.country
ORDER BY
  total_products_sold DESC;


-- Storing geographic information by city

SELECT
  geo.city,
  COUNT(DISTINCT user_pseudo_id) AS unique_users,
  SUM(item.quantity) AS total_products_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  geo.city
ORDER BY
  total_products_sold DESC;


-- Combining Seasonality and Geography
-- Calculating monthly sales by country

SELECT
  EXTRACT(MONTH
  FROM
    PARSE_DATE('%Y%m%d', CAST(event_date AS STRING))) AS sale_month,
  geo.country,
  COUNT(DISTINCT user_pseudo_id) AS unique_users,
  SUM(item.quantity) AS total_products_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  sale_month,
  geo.country
ORDER BY
  geo.country,
  sale_month;
