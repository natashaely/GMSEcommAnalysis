-- Examining the structure of the dataset

SELECT column_name, data_type
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'events_20210131' 


-- Querying a sample of the table’s data
  
SELECT *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
LIMIT 5

  
-- Counting the total number of rows in the table

SELECT COUNT(*) AS total_rows
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`


-- Counting the total number of columns in the table

SELECT 
    COUNT(*) AS total_columns
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'events_20210131';


-- Viewing a random sample of the data

SELECT *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE RAND() < 0.001


-- Counting the Distinct Values within the event_name column

SELECT COUNT(DISTINCT event_name) AS distinct_values
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`


-- Checking that all of the dates are in a consistent 'YYYYMMDD' format

SELECT event_date
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE SAFE.PARSE_DATE('%Y%m%d', event_date) IS NULL
