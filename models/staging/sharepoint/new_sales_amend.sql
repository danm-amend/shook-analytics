with new_sales as (
    select 
        cast("Month") as  as sale_month
    from 
        {{ source('sharepoint', 'new_sales_data_amend') }}
)

select * from new_sales