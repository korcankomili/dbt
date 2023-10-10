-- TODO: Add a test for the case where the 'duration' column is null/empty string.
-- TODO: If, in the future, a subscription name parsing is needed other than in Turkish, think what to do!

WITH stg AS (
    SELECT *
    FROM {{ ref('stg_itunes_connect__sales_subscriber_detailed') }}
)

, split_duration_string AS (
    SELECT * 
        , COALESCE(SPLIT_PART(standard_subscription_duration, ' ', 1), '1000') AS standard_subscription_duration_1
        , LOWER(COALESCE(SPLIT_PART(standard_subscription_duration, ' ', 2), 'Year')) AS standard_subscription_duration_2
        , COALESCE(SPLIT_PART(subscription_offer_duration, ' ', 1), '1000') AS subscription_offer_duration_1
        , LOWER(COALESCE(SPLIT_PART(subscription_offer_duration, ' ', 2), 'Year')) AS subscription_offer_duration_2
    FROM stg
)

, find_number_of_days AS (
    SELECT *
        , CASE 
            WHEN standard_subscription_duration_2 LIKE '%day%' THEN 1
            WHEN standard_subscription_duration_2 LIKE '%week%' THEN 7
            WHEN standard_subscription_duration_2 LIKE '%month%' THEN 30
            WHEN standard_subscription_duration_2 LIKE '%quarter%' THEN 90
            WHEN standard_subscription_duration_2 LIKE '%year%' THEN 365
        END AS standard_subscription_duration_2_number
        , CASE 
            WHEN subscription_offer_duration_2 LIKE '%day%' THEN 1
            WHEN subscription_offer_duration_2 LIKE '%week%' THEN 7
            WHEN subscription_offer_duration_2 LIKE '%month%' THEN 30
            WHEN standard_subscription_duration_2 LIKE '%quarter%' THEN 90
            WHEN subscription_offer_duration_2 LIKE '%year%' THEN 365
        END AS subscription_offer_duration_2_number
    FROM split_duration_string   
)

, calculate_total_days AS (
    SELECT * 
        , CAST(standard_subscription_duration_1 AS FLOAT) * standard_subscription_duration_2_number AS standard_subscription_durations
        , CAST(subscription_offer_duration_1 AS FLOAT) * subscription_offer_duration_2_number AS subscription_offer_durations
    FROM find_number_of_days
)

, choose_least AS (
    SELECT *
        , CASE 
            WHEN standard_subscription_durations < subscription_offer_durations
            THEN standard_subscription_duration
            WHEN subscription_offer_durations < standard_subscription_durations
            THEN subscription_offer_duration
            ELSE standard_subscription_duration
        END AS subscription_duration
    FROM calculate_total_days
)


, populate_missings_with_subscription_name AS (
    SELECT *,
        LOWER(CASE
            WHEN (subscription_duration IS NULL OR subscription_duration IN ('', ' ','NULL')) AND (subscription_name LIKE '%yıl%' OR subscription_name LIKE '%year%' OR LOWER(subscription_name) LIKE '%annual%') THEN '1 Year'
            WHEN (subscription_duration IS NULL OR subscription_duration IN ('', ' ','NULL')) AND (subscription_name LIKE '%6%month%' OR subscription_name LIKE '%6%ay%') THEN '6 Months'
            WHEN (subscription_duration IS NULL OR subscription_duration IN ('', ' ','NULL')) AND (subscription_name LIKE '%3%month%' OR subscription_name LIKE '%3%ay%') THEN '3 Months'
            WHEN (subscription_duration IS NULL OR subscription_duration IN ('', ' ','NULL')) AND (subscription_name LIKE '%monthly%' OR subscription_name LIKE '%aylık%') THEN '1 Month'
            WHEN (subscription_duration IS NULL OR subscription_duration IN ('', ' ','NULL')) AND (subscription_name LIKE '%week%' OR subscription_name LIKE '%hafta%') THEN '1 Week'
            ELSE subscription_duration
        END) AS duration
    FROM choose_least
)

SELECT 
      begin_date
    , purchase_date
    , app_apple_id
    , app_name
    , subscriber_id
    , country_code
    , customer_currency
    , proceeds_currency
    , device
    , proceeds_reason
    , refund
    , standard_subscription_duration
    , subscription_offer_duration
    , subscription_offer_type
    , duration
    , subscription_name
    , platform
    , units
    , proceeds
    , price
FROM populate_missings_with_subscription_name