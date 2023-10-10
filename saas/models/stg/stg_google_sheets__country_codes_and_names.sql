WITH source AS (
    SELECT
        *
    FROM {{ source('public', 'base_google_sheets__country_codes_and_names') }}
)

, ranked AS (
    SELECT * FROM (
        SELECT * 
        , ROW_NUMBER() OVER(PARTITION BY country_code, country_code_three_letters, country_name, language, language_code, currency_code ORDER BY _row) AS rn
        FROM source
    ) AS t
    WHERE rn = 1
)

, filtered AS (
    SELECT 
          country_code
        , country_code_three_letters
        , country_name
        , language
        , language_code
        , currency_code
    FROM ranked
    WHERE rn = 1
)

, lowered AS (
    SELECT 
          NULLIF(LOWER(country_code), '') AS country_code
        , NULLIF(LOWER(country_code_three_letters), '') AS country_code_three_letters
        , NULLIF(LOWER(country_name), '') AS country_name
        , NULLIF(LOWER(language), '') AS language
        , NULLIF(LOWER(language_code), '') AS language_code
        , NULLIF(LOWER(currency_code), '') AS currency_code
    FROM filtered
)

SELECT * FROM lowered