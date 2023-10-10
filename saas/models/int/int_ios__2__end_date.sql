WITH int_1 AS (
    SELECT *
FROM {{ ref('int_ios__1__duration') }}
)


, with_period_intervals AS (
  	SELECT *,
	  	SPLIT_PART(duration, ' ', 1) AS duration_period,
	    CASE
	      WHEN SPLIT_PART(duration, ' ', 2) LIKE '%day%' THEN 'day'
	      WHEN SPLIT_PART(duration, ' ', 2) LIKE '%week%' THEN 'week'
	      WHEN SPLIT_PART(duration, ' ', 2) LIKE '%month%' THEN 'month'
	      WHEN SPLIT_PART(duration, ' ', 2) LIKE '%quarter%' THEN 'quarter'
	      WHEN SPLIT_PART(duration, ' ', 2) LIKE '%year%' THEN 'year'
	    END AS duration_interval
	FROM int_1 
)


, with_end_date AS (
	SELECT *, 
    	CASE
    		WHEN duration_interval = 'day' THEN begin_date + (duration_period || ' days')::INTERVAL
	      	WHEN duration_interval = 'week' THEN begin_date + (duration_period || ' weeks')::INTERVAL
	      	WHEN duration_interval = 'month' THEN begin_date + (duration_period || ' months')::INTERVAL
	      	WHEN duration_interval = 'quarter' THEN begin_date + (duration_period || ' quarters')::INTERVAL
	      	WHEN duration_interval = 'year' THEN begin_date + (duration_period || ' years')::INTERVAL 
	    END AS end_date
	FROM with_period_intervals 
)


, duration_in_days AS (
	SELECT *,
		CAST(duration_period AS FLOAT) *
		CASE duration_interval
			WHEN 'day' THEN 1
			WHEN 'week' THEN 7
			WHEN 'month' THEN 30
			WHEN 'quarter' THEN 90
			WHEN 'year' THEN 365
		END AS duration_in_days
	FROM with_end_date
)


SELECT 
    begin_date
  , end_date
  , purchase_date
  , app_apple_id
  , app_name
  , subscriber_id
  , subscription_name
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
  , duration_in_days
  , platform
  , units
  , proceeds
  , price
FROM duration_in_days