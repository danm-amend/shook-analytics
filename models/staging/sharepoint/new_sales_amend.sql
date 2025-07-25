with new_sales as (
    select 
        *
    from 
        {{ source('sharepoint', 'new_sales_data_amend') }}
)

select * from new_sales