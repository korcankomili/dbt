WITH stg AS (
    SELECT * FROM {{ ref('int_ios__3__countries_and_currencies') }}
)


SELECT 
      app_apple_id
    , app_name
    , subscriber_id
    , subscription_name 
    , country
    , country_code
    , language
    , platform
    , standard_subscription_duration
    , duration
    , duration_in_days
    , begin_date
    , end_date
    , purchase_date
    , COALESCE(SUM(CASE WHEN refund AND price < 0 AND proceeds > 0 THEN proceeds*(-1) ELSE proceeds END),0) AS proceeds
    , COALESCE(SUM(price),0) AS price
    , COALESCE(SUM(CASE WHEN refund AND price < 0 AND proceed_usd > 0 THEN proceed_usd*(-1) ELSE proceed_usd END),0) AS proceed_usd
    , COALESCE(SUM(price_usd),0) AS price_usd 
FROM stg
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14