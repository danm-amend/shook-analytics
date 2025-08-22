with rf as (
    select 
        "forecast_type" as forecast_type,
        "category_type" as category_type,
        "category" as category,
        "revenue" as revenue,
        "margin" as margin,
        "pct_margin" as pct_margin
    from {{ get_active_rf_table() }}
)

select * from rf