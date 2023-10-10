{{
    dbt_utils.date_spine(
        datepart="day",
        start_date="'2016-01-01'::date",
        end_date="(current_date)::date + INTERVAL '1 year'"
    )
}}