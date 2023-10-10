WITH missing_conversion_rate_dates AS (
  SELECT
    *
  FROM {{ ref('util_days_with_1_year_future') }} AS dates
  LEFT JOIN {{ ref('stg_currencylayer__currencies_conversion_rates') }} AS cr
  ON dates.date_day = cr.effective_date
  WHERE dates.date_day <= CURRENT_DATE 
)


, fill_missing_dates_for_conversion_rates AS (
  SELECT
      date_day
    , COALESCE(missing_dates.currency_code, latest_rate.currency_code) AS currency_code
    , COALESCE(missing_dates.conversion_rate, latest_rate.conversion_rate) AS conversion_rate
  FROM missing_conversion_rate_dates missing_dates
  JOIN (
    SELECT *
    FROM (
      SELECT
        currency_code,
        MAX(conversion_rate) conversion_rate,
        ROW_NUMBER() OVER(PARTITION BY cr.currency_code ORDER BY effective_date DESC) AS row_numb
      FROM {{ ref('stg_currencylayer__currencies_conversion_rates') }} AS cr
      GROUP BY cr.currency_code, effective_date
      ) AS X
    WHERE row_numb = 1 
  ) AS latest_rate
  ON (missing_dates.currency_code = latest_rate.currency_code OR missing_dates.currency_code IS NULL) 
)

SELECT * FROM fill_missing_dates_for_conversion_rates