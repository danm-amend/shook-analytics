with pending_backlog as (
    -- select 
    --     *
    -- from 
    --     {{ source('sharepoint', 'pending_backlog_data_aug25') }}
    {{ union_pending_backlog_dynamic(source('metadata', 'pending_backlog_file_control'), 'shookdw', 'sharepoint') }}
), pending_cleaned as (
    select 
        {{first_of_month('"Month"')}} as month_date,
        "Region" as region,
        "Market Channel" as market_channel,
        "Job Name" as project,
        CASE
            WHEN trim("Pending Backlog (Revenue)") = '-' THEN null
            WHEN trim("Pending Backlog (Revenue)") like 'nan' THEN null
            WHEN "Pending Backlog (Revenue)" like '%(%)%' THEN -1 * to_number(replace(replace(replace("Pending Backlog (Revenue)", ',', ''), '(', ''), ')', ''))
            ELSE to_number(replace("Pending Backlog (Revenue)", ',', ''))
        END as pending_backlog_revenue,
        CASE
            WHEN "Pending Backlog (Margin)" like '%-%' THEN null
            WHEN trim("Pending Backlog (Margin)") like 'nan' THEN null
            WHEN "Pending Backlog (Margin)" like '%(%)%' THEN -1 * to_number(replace(replace(replace("Pending Backlog (Margin)", ',', ''), '(', ''), ')', ''))
            ELSE to_number(replace("Pending Backlog (Margin)", ',', ''))
        END as pending_backlog_margin
    from pending_backlog
)

select * from pending_cleaned