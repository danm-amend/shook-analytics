with rf as (
    select 
        forecast_type,
        category_type,
        category,
        revenue,
        margin,
        pct_margin,
        source_table,
        date(cast(file_month_year as timestamp)) as file_month_year, 
        TRY_TO_NUMBER(SPLIT_PART(forecast_type, ' ', 1)) as forecast_year
    from 
        {{ ref('rolling_forecast_summary') }}
    where category_type = 'Region'
), same_month_and_year as (
    select 
        file_month_year, forecast_year, 
        sum(revenue) as revenue,
        sum(margin) as margin
    from 
        rf
    -- where year(file_month_year) = forecast_year
    where forecast_year is not null
    group by file_month_year, forecast_year 
), params AS (
  SELECT
    DATE_TRUNC('month', MAX(file_month_year)) AS start_month,
    DATE_TRUNC('month', CURRENT_DATE) AS max_month
  FROM same_month_and_year
),
max_revenue AS (
  SELECT revenue, margin, forecast_year
  FROM same_month_and_year,
       params
  WHERE DATE_TRUNC('month', file_month_year) = params.start_month
--   and year(file_month_year) = year(params.start_month)
--   LIMIT 1
),
max_months AS (
  SELECT DATEADD(month, seq4(), dateadd(month, 1, p.start_month)) AS month,
--   r.revenue
  FROM TABLE(GENERATOR(ROWCOUNT => 13)) g, params p
--   JOIN max_revenue r
--   on year(month) = r.forecast_year
  WHERE DATEADD(month, seq4(), dateadd(month, 1, p.start_month)) <= p.max_month
--   and year(month) = year(p.start_month)
--   and month > p.start_month
    ORDER BY month
), join_month_rev as (
    select 
        * 
    from 
        max_months as m 
    join 
        max_revenue as r
    on year(m.month) = forecast_year 
), union_current_future as (
    select 
        *
    from 
        same_month_and_year
    where year(file_month_year) = forecast_year
    union all 
    select 
        month as file_month_year,
        forecast_year,
        revenue,
        margin
    from 
        join_month_rev
)
select * from union_current_future
order by file_month_year desc
-- select * from join_month_rev

-- SELECT
--   m.month,
--   r.revenue
-- FROM months m
-- CROSS JOIN max_revenue r
-- ORDER BY m.month