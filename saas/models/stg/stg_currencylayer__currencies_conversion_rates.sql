WITH sources AS (
    SELECT *
    FROM
        {{ source(
            'public',
            'base_currencylayer__currencies_conversion_rates'
        ) }}
)

/* Api currency layer stopped providing USD to USD constant 1 value after 2022-07-11 */
/* Also some of the timestamp columns do not provide consistent data, overrided using timestamp function */
, casts AS (
    SELECT
      	  CAST(currency_code AS TEXT) AS currency_code
    	, CAST(effective_date AS DATE) AS effective_date
    	, CAST(source AS TEXT) AS source
    	, CAST(conversion_rate AS FLOAT) AS conversion_rate
    	, CAST(insert_date AS TIMESTAMP) AS insert_date
    FROM sources
)

, lowered AS (
    SELECT 
          LOWER(currency_code) AS currency_code
        , effective_date
        , LOWER(source) AS source
        , conversion_rate
        , insert_date
    FROM casts
)

, usd_handle_helper AS (
    SELECT *
        , source = 'usd' AND currency_code = 'usd' AS is_usd2ust 
    FROM lowered
)

, usd_handle_remover AS (
    SELECT * 
    FROM usd_handle_helper
    WHERE NOT is_usd2ust
)

, distinct_dates AS (
    SELECT DISTINCT 
          'usd' as currency_code
        , effective_date
        , 'usd' AS source
        , 1 AS conversion_rate
        , CURRENT_TIMESTAMP AS insert_date
    FROM usd_handle_remover
)

SELECT 
	  currency_code
	, effective_date
	, source
	, conversion_rate
	, insert_date
FROM usd_handle_remover 
  UNION ALL
SELECT 
	  currency_code
	, effective_date
	, source
	, conversion_rate
	, insert_date
FROM distinct_dates
 