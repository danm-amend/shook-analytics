with backlog as (
    select 
        *
    from {{ ref('project_backlog') }}
), backlog_agg as (
    select 
        cast(file_month_year as date) as mth,
        sum(revenue_backlog) * 1000 as revenue_backlog,
        sum(ytd_revenue) as ytd_revenue
    from 
        backlog
    group by 
        file_month_year
), pending_backlog_agg as (
    select
        mth,
        sum(pending_backlog_revenue) as revenue_pending_backlog
    from 
        {{ ref('pending_backlog_prof') }}
    group by
        mth
), backlog_calcs as (
    select
        ba.mth,
        month(ba.mth) as mth_of_year,
        ba.revenue_backlog,
        pb.revenue_pending_backlog,
        ba.ytd_revenue,

        ba.revenue_backlog / ( ba.ytd_revenue / month(ba.mth) ) as executed_backlog_months,
        pb.revenue_pending_backlog/ ( ba.ytd_revenue / month(ba.mth) ) as pending_backlog_months
    from 
        backlog_agg as ba
        left join pending_backlog_agg as pb
            on ba.mth = pb.mth
)

select * from backlog_calcs
order by mth_of_year asc