with gl_actuals as (
    select 
        *
    from 
        {{ ref('gl_actuals') }}
), gl_budget as (
    select 
        *
    from 
        {{ ref('gl_budget') }}
), two_years_prior as (
    select 
        mth, 
        to_varchar(year(dateadd(year, -2, current_date))) || ' Revenue' as rev_type,
        region, 
        market,
        sum(netamt) as revenue
    from gl_actuals 
    where year(mth) = year(dateadd(year, -2, current_date))
    and include_company = 1 and region is not null
    and pl_line_item = 'construction_revenue'
    group by mth, region, market
), one_years_prior as (
    select 
        mth, 
        to_varchar(year(dateadd(year, -1, current_date))) || ' Revenue' as rev_type,
        region, 
        market,
        sum(netamt) as revenue
    from gl_actuals 
    where year(mth) = year(dateadd(year, -1, current_date))
    and include_company = 1 and region is not null
    and pl_line_item = 'construction_revenue'
    group by mth, region, market
), current_ytd as (
    select 
        mth, 
        to_varchar(year(current_date)) || ' YTD' as rev_type,
        region, 
        market,
        sum(netamt) as revenue
    from gl_actuals 
    where year(mth) = year(current_date)
    and include_company = 1 and region is not null
    and pl_line_item = 'construction_revenue'
    group by mth, region, market
), current_plan as (
    select 
        mth,
        to_varchar(year(current_date)) || ' Plan' as rev_type,
        region,
        market,
        sum(budget_amount) as revenue
    from gl_budget
    where mth_year = year(current_date) and budget_type = 'Plan'
    and include_company = 1 and region is not null
    and pl_line_item = 'construction_revenue'
    group by mth, region, market
), next_year_plan as (
    select 
        mth,
        to_varchar(year(dateadd(year, 1, current_date))) || ' Plan' as rev_type,
        region,
        market,
        sum(budget_amount) as revenue
    from gl_budget
    where mth_year = year(dateadd(year, 1, current_date)) and budget_type = 'Plan'
    and include_company = 1 and region is not null
    and pl_line_item = 'construction_revenue'
    group by mth, region, market
)
, current_FC as (
    select 
        mth,
        budget_name as rev_type,
        region,
        market,
        budget_amount
    from gl_budget
    where mth_year = year(current_date) and budget_type = 'FC'
    and include_company = 1 and region is not null
    and pl_line_item = 'construction_revenue'
    qualify fc_number = max(fc_number) over ()
    -- group by mth, region, market
), current_fc_grouped as (
    select
        mth,
        rev_type,
        region,
        market,
        sum(budget_amount) as revenue
    from current_FC 
    group by 
        mth,
        rev_type,
        region,
        market
), union_metrics as (
    select * from two_years_prior
    union all 
    select * from one_years_prior
    union all 
    select * from current_ytd
    union all 
    select * from current_plan
    union all 
    select * from next_year_plan
    union all 
    select * from current_fc_grouped
)

-- select rev_type, sum(revenue)
-- from union_metrics
-- group by rev_type

select * from union_metrics