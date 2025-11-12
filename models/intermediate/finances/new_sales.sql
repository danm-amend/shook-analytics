with new_sales as (
    select 
        *
    from 
        {{ ref('new_sales_amend') }}
)

select 
*
from new_sales

