with rf as (
    select 
        "forecast_type" as forecast_type,
        "category_type" as category_type,
        "category" as category,
        "revenue" as revenue,
        "margin" as margin,
        "pct_margin" as pct_margin,
        md.file_month_year as file_month_year
    from {{ get_active_rf_table() }}
    cross join 
    {{ source('metadata', 'rolling_forecast_file_control') }} as md
    where md.active = True

)

select * from rf