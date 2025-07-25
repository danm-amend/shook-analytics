with new_sales as (
    select 
        to_date("Month", 'MON-YY') as month_date,
        "Region" as region,
        marketchannel as market_channel,
        title as project,
        CASE
            WHEN plansales like '%-%' THEN null
            WHEN plansales like '%(%)%' THEN -1 * to_number(replace(replace(replace(plansales, ',', ''), '(', ''), ')', ''))
            ELSE to_number(replace(plansales, ',', ''))
        END as plan_sales,
        CASE
            WHEN actualsales like '%-%' THEN null
            WHEN actualsales like '%(%)%' THEN -1 * to_number(replace(replace(replace(actualsales, ',', ''), '(', ''), ')', ''))
            ELSE to_number(replace(actualsales, ',', ''))
        END as actual_sales,
        to_timestamp(created) as created_dt,
        to_timestamp(modified) as modified_dt
    from 
        {{ source('sharepoint', 'new_sales_data_amend') }}
)

select * from new_sales