-- Examining the structure of the dataset. 

SELECT column_name, data_type
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'events_20210131' 

-- Querying a sample of the table’s data

SELECT *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
LIMIT 5

-- Counting the total number of rows in the table

SELECT COUNT(*) AS total_rows
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`

-- Viewing a random sample of the data

SELECT *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
WHERE RAND() < 0.001

-- Counting the Distinct Values within the event_name column

SELECT COUNT(DISTINCT event_name) AS distinct_values
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`

-- Creating a new table with only the relevant columns (and UNNESTING the nested records)

CREATE TABLE `gmsproductanalysis.gmsdb.events_20210131` AS
SELECT
  event_date, 
  event_timestamp, 
  event_name, 
  event_params_unnest.key AS event_params_key,
  event_params_unnest.value.string_value AS event_params_string,
  CAST(event_params_unnest.value.int_value AS INT64) AS event_params_int,
  CAST(event_params_unnest.value.float_value AS INT64) AS event_params_float,
  CAST(event_params_unnest.value.double_value AS INT64) AS event_params_double,
  event_bundle_sequence_id,
  CAST(ROUND(event_value_in_usd) AS INT64) AS event_value_in_usd, 
  user_id, 
  CAST(ROUND(user_ltv.revenue) AS INT64) AS user_ltv_revenue,
  user_ltv.currency AS user_ltv_currency, 
  device.category AS device_category,
  device.mobile_brand_name AS mobile_brand_name,
  device.mobile_model_name AS mobile_model_name,
  device.mobile_os_hardware_model AS mobile_os_hardware_model,
  device.operating_system AS operating_system,
  device.web_info AS web_info,
  device.web_info.browser AS browser,
  device.web_info.browser_version AS browser_version,
  geo.continent AS continent,
  geo.country AS country, 
  geo.city AS city, 
  traffic_source.medium AS medium,
  traffic_source.name AS name,
  traffic_source.source AS source,
  platform, 
  ecommerce.total_item_quantity AS total_item_quantity, 
  CAST(ROUND(ecommerce.purchase_revenue_in_usd) AS INT64) AS purchase_revenue_in_usd,
  CAST(ROUND(ecommerce.purchase_revenue) AS INT64) AS purchase_revenue,
  ecommerce.unique_items AS unique_items,
  ecommerce.transaction_id AS transaction_id,
  items_unnest.item_id AS item_id,
  items_unnest.item_name AS item_name,
  items_unnest.item_brand AS item_brand,
  items_unnest.item_variant AS item_variant,
  items_unnest.item_category AS item_category,
  items_unnest.item_category2 AS item_category2,
  items_unnest.item_category3 AS item_category3,
  items_unnest.item_category4 AS item_category4,
  items_unnest.item_category5 AS item_category5,
  CAST(ROUND(items_unnest.price_in_usd) AS INT64) AS price_in_usd,
  CAST(ROUND(items_unnest.price) AS INT64) AS item_price,
  items_unnest.quantity AS item_quantity,
  CAST(ROUND(items_unnest.item_revenue_in_usd) AS INT64) AS item_revenue_in_usd,
  CAST(ROUND(items_unnest.item_revenue) AS INT64) AS item_revenue,
  items_unnest.coupon AS coupon,
  items_unnest.item_list_id AS item_list_id,
  items_unnest.item_list_name AS item_list_name,
  items_unnest.promotion_id AS promotion_id,
  items_unnest.promotion_name AS promotion_name,
  items_unnest.creative_name AS creative_name,
  items_unnest.creative_slot AS creative_slot
FROM 
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`,
  UNNEST(event_params) AS event_params_unnest,
  UNNEST(items) AS items_unnest
  
  
-- Counting the total number of rows in the table
  
SELECT COUNT(*) AS total_rows
FROM `gmsproductanalysis.gmsdb.events_20210131`
