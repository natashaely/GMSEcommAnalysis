-- Seasonality Analysis
-- Breaking down sales data every month

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


-- Breaking down sales data every week

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

-- Average item price per country

SELECT
  geo.country,
  AVG(item.price_in_usd) AS avg_price
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
GROUP BY
  geo.country
ORDER BY
  avg_price DESC;
  
  
-- The average basket size per country (in USD)

WITH PurchaseTotals AS (
  SELECT
    geo.country,
    SUM(item.price) AS total_purchase_value
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` events,
    UNNEST(events.items) AS item
  WHERE
    event_name = 'purchase'
  GROUP BY
    events.event_bundle_sequence_id, -- Assuming this uniquely identifies a purchase
    geo.country
)

SELECT
  country,
  AVG(total_purchase_value) AS avg_purchase_value_per_country
FROM
  PurchaseTotals
GROUP BY
  country
ORDER BY
  avg_purchase_value_per_country DESC;


-- The most active and valuable customers per country 

WITH BestSellers AS (
  SELECT
    item.item_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS item
  WHERE
    event_name = 'purchase'
  GROUP BY
    item.item_id
  ORDER BY
    COUNT(*) DESC
  LIMIT
    10
)

SELECT
  geo.country,
  COUNT(*) AS count,
  AVG(events.user_ltv.revenue) AS avg_ltv
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` events,
  UNNEST(events.items) AS item
JOIN
  BestSellers
ON
  item.item_id = BestSellers.item_id
GROUP BY
  geo.country
ORDER BY
  count DESC;


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
