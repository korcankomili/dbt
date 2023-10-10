{{ config(
materialized = 'table',
partition_by = {
  "field": "begin_date",
  "data_type": "DATE"
})}}

WITH sources AS (
    SELECT *
    FROM {{ source('public', 'base_itunes_connect__sales_subscriber_detailed') }} 
)

, renamed_and_filtered AS (
  SELECT
      CASE
        WHEN event_date in ('', ' ') or event_date IS NULL THEN DATE('1900-01-01')
        ELSE CAST(event_date AS DATE)
      END AS begin_date 
    , CASE
        WHEN purchase_date in ('', ' ') or purchase_date IS NULL THEN DATE('1900-01-01')
        ELSE CAST(purchase_date AS DATE)
      END AS purchase_date 
    , app_apple_id
    , NULLIF(LOWER(app_name), '') AS app_name
    , NULLIF(CAST(subscriber_id AS TEXT), '') AS subscriber_id
    , NULLIF(LOWER(country), '') AS country_code
    , NULLIF(LOWER(customer_currency), '') AS customer_currency
    , NULLIF(LOWER(proceeds_currency), '') AS proceeds_currency
    , NULLIF(LOWER(device), '') AS device
    , CASE
        WHEN proceeds_reason IS NULL OR proceeds_reason IN ('', ' ','NULL') THEN 'none'
        ELSE LOWER(proceeds_reason)
      END AS proceeds_reason
    , CASE WHEN LOWER(COALESCE(refund, 'no')) = 'yes' THEN TRUE ELSE FALSE END AS refund
    , NULLIF(LOWER(standard_subscription_duration), '') AS standard_subscription_duration
    , NULLIF(LOWER(subscription_offer_duration), '') AS subscription_offer_duration
    , NULLIF(LOWER(subscription_offer_type), '') AS subscription_offer_type
    , REPLACE(NULLIF(LOWER(subscription_name), ''), '?', 'ı') AS subscription_name
    , 'ios' as platform
    , SUM(CAST(units AS FLOAT)) AS units
    , SUM(CAST(developer_proceeds AS FLOAT)) AS proceeds
    , SUM(CAST(customer_price AS FLOAT)) AS price
  FROM sources 
  WHERE app_apple_id <> '1144930470'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

SELECT * FROM renamed_and_filtered