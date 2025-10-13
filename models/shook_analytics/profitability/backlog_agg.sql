with backlog as (
    select 
        *
    from {{ ref('project_backlog') }}
), backlog_agg as (
    select 
        cast(file_month_year as date) as file_month_year,
        sum(revenue_backlog) * 1000 as revenue_backlog,
        sum(ytd_revenue) as ytd_revenue
    from 
        backlog
    group by file_month_year
), backlog_calcs as (
    select
        file_month_year,
        month(file_month_year) as mth_of_year,
        revenue_backlog,
        ytd_revenue,
        revenue_backlog / ( ytd_revenue / month(file_month_year) ) as backlog_months
    from 
        backlog_agg
)

select * from backlog_calcs