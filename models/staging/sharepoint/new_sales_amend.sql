with union_sales as (
    {{ union_sales_dynamic(source('metadata', 'new_sales_file_control'), 'shookdw', 'sharepoint') }}
), col_clean as (
    select 
        {{first_of_month('"Month"')}} as month_date,
        "Region" as region,
        "Market Channel" as market_channel,
        "Title" as project,
        CASE
            WHEN "Plan Sales" like '%-%' THEN null
            WHEN trim("Plan Sales") like 'nan' THEN null
            WHEN "Plan Sales" like '%(%)%' THEN -1 * to_number(replace(replace(replace("Plan Sales", ',', ''), '(', ''), ')', ''))
            ELSE to_number(replace("Plan Sales", ',', ''))
        END as plan_sales,
        CASE
            WHEN "Actual Sales" like '%-%' THEN null
            WHEN trim("Actual Sales") like 'nan' THEN null
            WHEN "Actual Sales" like '%(%)%' THEN -1 * to_number(replace(replace(replace("Actual Sales", ',', ''), '(', ''), ')', ''))
            ELSE to_number(replace("Actual Sales", ',', ''))
        END as actual_sales,
    from union_sales 
)

select * from col_clean