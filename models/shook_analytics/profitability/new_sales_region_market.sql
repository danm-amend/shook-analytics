with new_sales as (
    select 
        *
    from 
        {{ ref('new_sales') }}
)

select * from new_sales