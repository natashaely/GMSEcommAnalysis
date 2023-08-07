-- Counting all purchase events

SELECT
  COUNT(DISTINCT event_bundle_sequence_id) AS purchase_events
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'purchase'


-- Counting all distinct transaction IDs

SELECT
  COUNT(DISTINCT ecommerce.transaction_id) AS transaction_ids
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`


-- Counting the total number of products purchased with coupon

SELECT
  COUNT(item.item_id) AS num_products
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
  AND item.coupon IS NOT NULL
  AND item.promotion_id != '';


-- Counting the number of unique products purchased with coupon

SELECT
  COUNT(DISTINCT item.item_id) AS unique_products
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
  AND item.coupon IS NOT NULL
  AND item.promotion_id != '';


-- Checking if there are any non-discounted products in the dataset

SELECT
  CASE
    WHEN item.coupon = '' THEN 'No Coupon'
  ELSE
  'Has Coupon'
END
  AS coupon_status,
  COUNT(DISTINCT item.item_id) AS num_products,
  SUM(item.quantity) AS total_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  coupon_status;


-- Counting total sales where a coupon was used

SELECT
  item.coupon AS coupon_code,
  COUNT(DISTINCT user_pseudo_id) AS number_of_users,
  SUM(item.quantity) AS total_quantity,
  SUM(item.quantity * item.price) AS total_sales
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  coupon_code
ORDER BY
  total_sales DESC;


-- Average order value where a coupon was used and not used

SELECT
  CASE
    WHEN item.coupon IS NOT NULL AND item.coupon != '' THEN 'With Coupon'
  ELSE
  'Without Coupon'
END
  AS order_type,
  COUNT(DISTINCT event_bundle_sequence_id) AS number_of_orders,
  SUM(item.quantity * item.price) AS total_sales,
  SUM(item.quantity * item.price) / COUNT(DISTINCT event_bundle_sequence_id) AS average_order_value
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  order_type
ORDER BY
  order_type DESC;


-- Reviewing the sales performance of promoted products
-- Identifying products that have been discounted

WITH
  DiscountedProducts AS (
  SELECT
    DISTINCT item.item_id AS unique_product
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS item
  WHERE
    item.coupon IS NOT NULL
    AND item.coupon != '' )

-- Aggregating quantities sold with and without a discount for those products

SELECT
  d.unique_product,
  SUM(CASE
      WHEN item.coupon IS NOT NULL AND item.coupon != '' THEN item.quantity
    ELSE
    0
  END
    ) AS sold_on_discount,
  SUM(CASE
      WHEN item.coupon IS NOT NULL AND item.coupon = '' THEN item.quantity
    ELSE
    0
  END
    ) AS sold_without_discount
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t,
  UNNEST(items) AS item
JOIN
  DiscountedProducts d
ON
  item.item_id = d.unique_product
WHERE
  t.event_name = 'purchase'
GROUP BY
  d.unique_product;


-- Products sold with promotions and those without

SELECT
  CASE
    WHEN item.coupon IS NOT NULL AND item.coupon != '' THEN 'Discounted'
  ELSE
  'Not Discounted'
END
  AS promotion_status,
  COUNT(DISTINCT item.item_id) AS num_products,
  SUM(item.quantity) AS total_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  promotion_status
ORDER BY
  promotion_status;


-- Reviewing overall sales performance on promoted products
-- Products with promotions 

SELECT
  CASE
    WHEN item.coupon IS NOT NULL AND item.coupon != '' THEN TRUE
  ELSE
  FALSE
END
  AS is_discounted,
  AVG(item.quantity) AS avg_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  is_discounted;


-- Reviewing average sales volume for products with and without promotions

SELECT
  CASE
    WHEN item.coupon IS NOT NULL AND item.coupon != '' THEN 'With Coupon'
  ELSE
  'Without Coupon'
END
  AS order_type,
  AVG(item.quantity) AS avg_sold
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
WHERE
  event_name = 'purchase'
GROUP BY
  order_type;
