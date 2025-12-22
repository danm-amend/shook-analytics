with rf as (
    {{ union_tables_dynamic(source('metadata', 'rolling_forecast_file_control'), 'shookdw', 'sharepoint') }}
), rf_cleaned as (
    select  
        "forecast_type" as forecast_type,
        "category_type" as category_type,
        "category" as category,
        "revenue" as revenue,
        "margin" as margin,
        "pct_margin" as pct_margin,
        source_table,
        file_month_year
    from rf
)

select * from rf_cleaned