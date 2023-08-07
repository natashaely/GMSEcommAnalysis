-- Top 10 items added to cart by most users

SELECT
  item.item_id,
  item.item_name,
  COUNT(DISTINCT user_pseudo_id) AS user_count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'add_to_cart'
GROUP BY
  1,
  2
ORDER BY
  user_count DESC
LIMIT
  10;


-- Top 10 best-selling items 

SELECT
  item.item_id,
  item.item_name,
  SUM(item.quantity) AS total_sold_quantity
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  1,
  2
ORDER BY
  total_sold_quantity DESC
LIMIT
  10;


-- Calculating the average price of the best-selling products

WITH
  TopItems AS (
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
    10 )
SELECT
  AVG(item.price) AS avg_price
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  item.item_id IN (
  SELECT
    item_id
  FROM
    TopItems);


-- Distribution of best-selling products by category

SELECT
  item.item_category,
  COUNT(*) AS count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  item.item_id IN (
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
    10)
GROUP BY
  item.item_category;


-- Common shared characteristics of best-sellers (category)

SELECT
  item.item_category,
  COUNT(*) AS count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  item.item_id IN (
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
    10 )
  AND item.item_category <> '(not set)'
GROUP BY
  item.item_category
ORDER BY
  count DESC;


-- Common shared characteristics of best-sellers (price). 

SELECT
  item.price_in_usd,
  COUNT(*) AS count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  item.item_id IN (
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
    10 )
  AND item.price_in_usd IS NOT NULL
GROUP BY
  item.price_in_usd
ORDER BY
  count DESC;


-- Geographic distribution of buyers for best-sellers

SELECT
  geo.country,
  COUNT(*) AS count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  item.item_id IN (
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
    10 )
GROUP BY
  geo.country
ORDER BY
  count DESC;


-- Device category of buyers for best-sellers

SELECT
  device.category,
  COUNT(*) AS count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  item.item_id IN (
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
    10 )
GROUP BY
  device.category
ORDER BY
  count DESC;


-- Price and sales correlation: Analysing how price impacts sales

SELECT
  price_range,
  COUNT(*) AS total_sales,
  AVG(item_revenue_in_usd) AS avg_sales
FROM (
  SELECT
    item.item_id,
    item.item_name,
    CASE
      WHEN item.price_in_usd < 50 THEN '0-50'
      WHEN item.price_in_usd BETWEEN 50
    AND 100 THEN '50-100'
    ELSE
    '100+'
  END
    AS price_range,
    item.item_revenue_in_usd
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS item )
GROUP BY
  price_range
ORDER BY
  total_sales DESC;


-- Price over time: Examining the average price per month or per quarter

SELECT
  FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP_MICROS(event_timestamp)) AS month,
  AVG(item.price_in_usd) AS avg_price
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
GROUP BY
  month
ORDER BY
  month ASC;


-- Price and geography: Exploring geographic trends in pricing

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
  
  
  -- Items Frequently Purchased Together

WITH
  item_pairs AS (
  SELECT
    i1.item_name AS item1,
    i2.item_name AS item2,
    COUNT(DISTINCT t1.event_bundle_sequence_id) AS pair_count
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t1
  CROSS JOIN
    UNNEST(t1.items) AS i1
  JOIN
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t2
  ON
    t1.event_bundle_sequence_id = t2.event_bundle_sequence_id
  CROSS JOIN
    UNNEST(t2.items) AS i2
  WHERE
    t1.event_name = 'purchase'
    AND t2.event_name = 'purchase'
    AND i1.item_name < i2.item_name
  GROUP BY
    item1,
    item2 )
SELECT
  item1,
  item2,
  pair_count
FROM
  item_pairs
ORDER BY
  pair_count DESC
LIMIT
  10;
