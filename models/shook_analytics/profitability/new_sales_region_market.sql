with new_sales as (
    select 
        *
    from 
        {{ ref('new_sales') }}
    -- where year(month_date) = year(current_timestamp())
)

select * from new_sales