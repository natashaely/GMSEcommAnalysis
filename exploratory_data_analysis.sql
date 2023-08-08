-- View all event types

SELECT
  DISTINCT event_name
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`


-- Summary statistics: Event values for purchase events

SELECT
  MIN(event_value_in_usd) AS min_value,
  MAX(event_value_in_usd) AS max_value,
  AVG(event_value_in_usd) AS avg_value,
  STDDEV(event_value_in_usd) AS stddev_value
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'purchase'


-- Summary statistics: Purchase revenue in USD

SELECT
  MIN(ecommerce.purchase_revenue_in_usd) AS min_value,
  MAX(ecommerce.purchase_revenue_in_usd) AS max_value,
  AVG(ecommerce.purchase_revenue_in_usd) AS avg_value,
  STDDEV(ecommerce.purchase_revenue_in_usd) AS stddev_value
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`


-- Summary statistics: Examining price distribution

SELECT 
    MIN(item.price_in_usd) AS min_value,
    MAX(item.price_in_usd) AS max_value,
    AVG(item.price_in_usd) AS avg_value,
    STDDEV(item.price_in_usd) AS stddev_value
FROM 
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS item


-- Summary statistics: Item revenue

SELECT
  MIN(item.item_revenue_in_usd) AS min_value,
  MAX(item.item_revenue_in_usd) AS max_value,
  AVG(item.item_revenue_in_usd) AS avg_value,
  STDDEV(item.item_revenue_in_usd) AS stddev_value
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item


-- Summary statistics: User lifetime revenue

SELECT
  MIN(user_ltv.revenue) AS min_value,
  MAX(user_ltv.revenue) AS max_value,
  AVG(user_ltv.revenue) AS avg_value,
  STDDEV(user_ltv.revenue) AS stddev_value
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

    
-- Distribution of categorical variables
-- Distribution of events

SELECT
  event_name,
  COUNT(*) AS event_count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
GROUP BY
  event_name
ORDER BY
  event_count DESC;


-- Distribution of product items

SELECT
  item.item_name,
  COUNT(*) AS item_count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
GROUP BY
  item.item_name
ORDER BY
  item_count DESC;


-- Distribution of item categories

SELECT
  item.item_category,
  COUNT(*) AS cat_count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
GROUP BY
  item.item_category
ORDER BY
  cat_count DESC;


-- Distribution of item brand 

SELECT
  item.item_brand,
  COUNT(*) AS brand_count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
GROUP BY
  item_brand
ORDER BY
  brand_count DESC;


-- Distribution of promotions

SELECT
  item.promotion_name,
  COUNT(*) AS promo_count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
GROUP BY
  item.promotion_name
ORDER BY
  promo_count DESC;


-- Analysing numeric values
-- Analysing event values

SELECT
  MIN(event_value_in_usd) AS min_value,
  MAX(event_value_in_usd) AS max_value,
  AVG(event_value_in_usd) AS avg_value,
  SUM(event_value_in_usd) AS total_value
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;


-- Analysing purchase revenue 

SELECT
  MIN(ecommerce.purchase_revenue_in_usd) AS min_value,
  MAX(ecommerce.purchase_revenue_in_usd) AS max_value,
  AVG(ecommerce.purchase_revenue_in_usd) AS avg_value,
  SUM(ecommerce.purchase_revenue_in_usd) AS total_value
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;


-- Analysing item revenue

SELECT
  MIN(item.item_revenue_in_usd) AS min_value,
  MAX(item.item_revenue_in_usd) AS max_value,
  AVG(item.item_revenue_in_usd) AS avg_value,
  SUM(item.item_revenue_in_usd) AS total_value
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;


-- Calculating the frequency of purchase events and an estimated average conversion rate 

WITH
  EventCounts AS (
  SELECT
    SUM(CASE
        WHEN event_name = 'add_to_cart' THEN 1
      ELSE
      0
    END
      ) AS addToCartCount,
    SUM(CASE
        WHEN event_name = 'purchase' THEN 1
      ELSE
      0
    END
      ) AS purchaseCount
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` )
SELECT
  purchaseCount,
  addToCartCount,
  (purchaseCount / addToCartCount) * 100 AS conversionRate
FROM
  EventCounts;
