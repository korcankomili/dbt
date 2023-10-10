WITH int_2 AS (
    SELECT *
    FROM {{ ref('int_ios__2__end_date') }}
)


, with_countries AS (
  SELECT 
      stg.*
    , COALESCE(countries.country_name, stg.country_code) as country
    , COALESCE(countries.language, 'unknown') as language
  FROM int_2 AS stg
  LEFT JOIN {{ ref('stg_google_sheets__country_codes_and_names') }} AS countries
  ON LOWER(stg.country_code) = LOWER(countries.country_code)
)

, currency_conversions AS (
    SELECT * FROM {{ ref('dim_currency_conversions') }}
)

, with_USD AS (
    SELECT 
          stg.*
        , stg.proceeds * (1 / curr.conversion_rate) AS proceed_usd
        , stg.price * (1 / curr.conversion_rate) AS price_usd
    FROM with_countries AS stg
    JOIN currency_conversions curr 
     ON stg.begin_date = curr.date_day
    AND stg.proceeds_currency = curr.currency_code
)

SELECT * FROM with_USD