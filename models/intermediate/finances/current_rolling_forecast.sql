with rf as (
    select 
        *
    from 
        {{ ref('rolling_forecast_summary') }}
), relevent_data as (
    select 
        *
    from 
        rf
    where category_type = 'Region'
    and TRY_TO_NUMBER(SPLIT_PART(forecast_type, ' ', 1)) = EXTRACT(YEAR FROM CURRENT_DATE)
)

select * from relevent_data