{# with new_sales as (
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
) #}

with may as (
    select 
        to_date("Month", 'YYYY-MM-DD') as "Month",
        "Region",
        "Market Channel",
        "Project",
        "Plan Sales",
        "Actual Sales"
    from 
        {{ source('sharepoint', 'new_sales_data_may25') }}
), jun as (
    select 
        to_date("Month", 'MON-YY') as "Month",
        "Region",
        "Market Channel",
        "Project",
        "Plan Sales",
        "Actual Sales"
    from 
        {{ source('sharepoint', 'new_sales_data_jun25') }}
), jul as (
    select 
        to_date("Month", 'YYYY-MM-DD') as "Month",
        "Region",
        "Market Channel",
        "Title" as "Project",
        "Plan Sales",
        "Actual Sales"
    from 
        {{ source('sharepoint', 'new_sales_data_jul25') }}
), tab_union as (
    select * from may
    union all
    select * from jun
    union all 
    select * from jul
), cleaned_tab as (
    select 
        "Month" as month_date,
        "Region" as region,
        "Market Channel" as market_channel,
        "Project" as project,
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
    from
        tab_union
)


select * from cleaned_tab