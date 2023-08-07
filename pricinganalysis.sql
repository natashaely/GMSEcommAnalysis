-- Identifying the top 5 best-selling items 

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
  5;


-- Analysing the pricing pattern for the top 5 best-selling items over time

WITH
  Top5Products AS (
  SELECT
    item.item_id AS product_sku
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS item
  WHERE
    event_name = 'purchase'
  GROUP BY
    item.item_id
  ORDER BY
    SUM(item.quantity) DESC
  LIMIT
    5 )
SELECT
  item.item_id AS product_sku,
  PARSE_DATE('%Y%m%d', CAST(t.event_date AS STRING)) AS purchase_day,
  AVG(item.price) AS avg_daily_price
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t,
  UNNEST(items) AS item
JOIN
  Top5Products tp
ON
  item.item_id = tp.product_sku
WHERE
  t.event_name = 'purchase'
GROUP BY
  item.item_id,
  purchase_day
ORDER BY
  item.item_id,
  purchase_day;


-- Analysing the sales volume relative to price changes for the top 5 best-selling items over time

WITH
  Top5Products AS (
  SELECT
    item.item_id AS product_sku
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS item
  WHERE
    event_name = 'purchase'
  GROUP BY
    item.item_id
  ORDER BY
    SUM(item.quantity) DESC
  LIMIT
    5 )
SELECT
  item.item_id AS product_sku,
  PARSE_DATE('%Y%m%d', CAST(t.event_date AS STRING)) AS purchase_day,
  AVG(item.price) AS avg_daily_price,
  SUM(item.quantity) AS daily_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t,
  UNNEST(items) AS item
JOIN
  Top5Products tp
ON
  item.item_id = tp.product_sku
WHERE
  t.event_name = 'purchase'
GROUP BY
  item.item_id,
  purchase_day
ORDER BY
  item.item_id,
  purchase_day;
