with backlog as (
    select 
        *
    from {{ ref('project_backlog') }}
), backlog_agg as (
    select 
        file_month_year,
        sum(revenue_backlog) * 1000 as revenue_backlog
    from 
        backlog
    group by file_month_year
)

select * from backlog_agg