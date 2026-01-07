with gl_actuals as (
    select 
        mth,
        -1 * sum(iff(pl_line_item = 'construction_revenue' and region is not null, netamt, 0)) as revenue,
        sum(iff(pl_line_item = 'direct_construction_cost' and region is not null, netamt, 0)) as direct_cost,
        sum(iff(pl_line_item = 'indirect_construction_cost' and use_indirect_cost = 1, netamt, 0)) as indirect_cost
    from 
        {{ ref('gl_actuals') }}
    where include_company = 1
    and year(mth) >= 2023
    group by mth
), gl_budget as (
    select 
        mth,
        budget_type,
        budget_name,
        fc_number,
        mth_year,
        -1 * sum(iff(pl_line_item = 'construction_revenue' and region is not null, budget_amount, 0)) as revenue,
        sum(iff(pl_line_item = 'direct_construction_cost' and region is not null, budget_amount, 0)) as direct_cost,
        sum(iff(pl_line_item = 'indirect_construction_cost' and use_indirect_cost = 1, budget_amount, 0)) as indirect_cost
    from 
        {{ ref('gl_budget') }}
    where include_company = 1
    and mth_year >= 2023
    group by mth, budget_type, budget_name, fc_number, mth_year
)
-- , actuals_metric as (
--     select 
--         mth, 
--         'gl_actuals' as rev_type,
--         -- to_varchar(year(dateadd(year, -2, current_date))) as rev_type,
--         -- region, 
--         -- market,
--         revenue,
--         direct_cost,
--         indirect_cost,
--         revenue - direct_cost - indirect_cost as margin
--     from gl_actuals 
--     -- where year(mth) = year(dateadd(year, -2, current_date))    
-- )
, two_years_prior as (
    select 
        mth, 
        --to_varchar(year(dateadd(year, -2, current_date))) || ' Metrics' as rev_type,
        to_varchar(year(dateadd(year, -2, current_date))) as rev_type,
        -- region, 
        -- market,
        revenue,
        direct_cost,
        indirect_cost,
        revenue - direct_cost - indirect_cost as margin
    from gl_actuals 
    where year(mth) = year(dateadd(year, -2, current_date))
), one_years_prior as (
    select 
        mth, 
        --to_varchar(year(dateadd(year, -1, current_date))) || ' Metrics' as rev_type,
        to_varchar(year(dateadd(year, -1, current_date))) as rev_type,
        -- region, 
        -- market,
        revenue,
        direct_cost,
        indirect_cost,
        revenue - direct_cost - indirect_cost as margin
    from gl_actuals 
    where year(mth) = year(dateadd(year, -1, current_date))
), current_ytd as (
    select 
        mth, 
        to_varchar(year(current_date)) || ' YTD' as rev_type,
        -- region, 
        -- market,
        revenue,
        direct_cost,
        indirect_cost,
        revenue - direct_cost - indirect_cost as margin
    from gl_actuals 
    where year(mth) = year( current_date)
)
, current_plan as (
    select 
        mth,
        to_varchar(year(current_date)) || ' Plan' as rev_type,
        revenue,
        direct_cost,
        indirect_cost,
        revenue - direct_cost - indirect_cost as margin
    from gl_budget
    where mth_year = year(current_date) and budget_type = 'Plan'
    qualify fc_number = max(fc_number) over ()
)
, next_year_plan as (
    select 
        mth,
        to_varchar(year(dateadd(year, 1, current_date))) || ' Plan' as rev_type,
        revenue,
        direct_cost,
        indirect_cost,
        revenue - direct_cost - indirect_cost as margin
    from gl_budget
    where mth_year = year(dateadd(year, 1, current_date)) and budget_type = 'Plan'
    qualify fc_number = max(fc_number) over ()
), current_FC as (
    select 
        mth,
        budget_name as rev_type,
        revenue,
        direct_cost,
        indirect_cost,
        revenue - direct_cost - indirect_cost as margin
    from gl_budget
    where mth_year = year(current_date) and budget_type = 'FC'
    --qualify fc_number = max(fc_number) over ()
), rolling_fc as (
    select
        -- DATE_TRUNC('MONTH', CURRENT_DATE) AS mth,
        file_month_year as mth,
        --forecast_type as rev_type,
        -- replace(forecast_type, ' Rolling Forecast', ' RF') as rev_type,
        'Rolling Forecast' as rev_type,
        -- round(sum(revenue), 2) as revenue,
        revenue,
        null as direct_cost,
        null as indirect_cost,
        margin
        -- round(sum(margin), 2) as margin
    from 
        {{ ref('current_rolling_forecast') }}
    -- group by forecast_type, mth
)
, union_metrics as (
    select * from two_years_prior
    union all 
    select * from one_years_prior
    union all 
    select * from current_ytd
    union all 
    -- select * from actuals_metric
    -- union all
    select * from current_plan
    union all 
    select * from next_year_plan
    union all 
    select * from current_FC
    union all 
    select * from rolling_fc
)

select * from union_metrics
order by rev_type, mth desc
