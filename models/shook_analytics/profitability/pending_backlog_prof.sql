with pending_backlog as (
    select 
        to_date(month_date) as mth,
        region as region_name,
        market_channel as market_name,
        project,
        cast(pending_backlog_revenue as float) as pending_backlog_revenue,
        cast(pending_backlog_margin as float) as pending_backlog_margin
    from 
        {{ ref('pending_backlog') }}
)

select * from pending_backlog