with wip_union as (
    select
        * 
    from 
        {{ ref('wip_union') }}
), wip_backlog as (
    select 
    "Contracts Job Number" as job_number,
    "Contracts Contract Description" as job_description,
    "region" as region,
    file_month_year, 
    "Backlog in 000's Revenue" as revenue_backlog,
    "Year Revenue" as ytd_revenue
    from wip_union
)

select * from wip_backlog