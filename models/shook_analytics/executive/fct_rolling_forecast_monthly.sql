with rolling_fc as (
    select
        -- DATE_TRUNC('MONTH', CURRENT_DATE) AS mth,
        file_month_year as mth,
        --forecast_type as rev_type,
        -- replace(forecast_type, ' Rolling Forecast', ' RF') as rev_type,
        'Rolling Forecast' as rev_type,
        -- round(sum(revenue), 2) as revenue,
        revenue,
        null as direct_cost,
        null as indirect_cost,
        margin
        -- round(sum(margin), 2) as margin
    from 
        {{ ref('current_rolling_forecast') }}
    -- group by forecast_type, mth
)

select * from rolling_fc