with new_sales as (
    select 
        'New Sales' as data_source,
        file_month_year as data_date_ref,
        first_load_dt,
        last_load_dt
    from 
        {{ source('metadata', 'new_sales_file_control') }}
    where active = True
    qualify row_number() over (partition by null order by data_date_ref desc) = 1
), wip as (
    select 
        'WIP' as data_source,
        file_month_year as data_date_ref,
        first_load_dt,
        last_load_dt
    from 
        {{ source('metadata', 'wip_file_control') }}
    where active = True
    qualify row_number() over (partition by null order by data_date_ref desc) = 1
), rolling_fc as (
        select 
        'Rolling Forecast' as data_source,
        file_month_year as data_date_ref,
        first_load_dt,
        last_load_dt
    from 
        {{ source('metadata', 'rolling_forecast_file_control') }}
    where active = True
    qualify row_number() over (partition by null order by data_date_ref desc) = 1
), pending_bl as (
        select 
        'Pending Backlog' as data_source,
        file_month_year as data_date_ref,
        first_load_dt,
        last_load_dt
    from 
        {{ source('metadata', 'pending_backlog_file_control') }}
    where active = True
    qualify row_number() over (partition by null order by data_date_ref desc) = 1
), kpa as (
    select 
        'KPA' as data_source,
        injury_date as data_month_ref,
        convert_timezone('America/Los_Angeles', 'America/New_York', first_load_dt) AS first_load_dt,
        convert_timezone('America/Los_Angeles', 'America/New_York', last_load_dt) AS last_load_dt
    from 
        {{ ref('osha_recordable_incidents') }}
    CROSS JOIN 
    (
        select min(last_load_dt) as first_load_dt, max(last_load_dt) as last_load_dt from {{ source('metadata', 'kpa_refresh_log') }} where success = true
    )
    qualify row_number() over (partition by null order by injury_date desc) = 1
), gl_actuals_refresh as (
    select 
        last_altered
    from 
        shookdw.information_schema.tables
    where 
        table_schema = 'VIEWPOINT'
        and table_name = 'GLAS'
), gl_actuals as (
    select 
        'GL Actuals' as data_source,
        date(max(actual_date)) as data_date_ref,
        null as first_load_dt,
        convert_timezone('America/Los_Angeles', 'America/New_York', max(b.last_altered)) as last_load_dt
    from intermediate.finances.actual_cost as a 
    CROSS JOIN 
    gl_actuals_refresh AS b
    where date(actual_date) <= date(convert_timezone('America/Los_Angeles', 'America/New_York', current_timestamp())) 
    -- to_char(current_date, 'YYYY-MM-01') 
), gl_budget_max_fc as (
    select
    budget_name
    from {{ ref('gl_budget') }} 
    where budget_type = 'FC'
    qualify row_number() over (order by mth_year desc, fc_number desc) = 1
), gl_budget_refresh as (
    select 
        last_altered
    from 
        shookdw.information_schema.tables
    where 
        table_schema = 'VIEWPOINT'
        and table_name = 'GLBD'
), gl_budget as (
    select 
        'GL Budget' as data_source,
        min(MTH) as data_date_ref,
        null as first_load_dt,
        convert_timezone('America/Los_Angeles', 'America/New_York', max(c.last_altered)) as last_load_dt
    from 
        {{ ref('gl_budget') }} as a 
    join gl_budget_max_fc as b
    on a.budget_name = b.budget_name
    cross join 
    gl_budget_refresh as c 
    where budget_type = 'FC'
), unanet as (
    select 
        'Unanet' as data_source,
        data_date_ref,
        first_load_dt,
        last_load_dt
    from
    (
        select
            min(last_load_dt) as first_load_dt,
            max(last_load_dt) as last_load_dt
        from {{ source('metadata', 'unanet_refresh_log') }}
    ) as a 
    cross join 
    (
        select max(to_timestamp(last_modified_date_time)) as data_date_ref
            from
        {{ ref('int_opportunities') }} 
    ) as b
), metric_union as (
    select * from new_sales
    union all
    select * from wip 
    union all 
    select * from rolling_fc
    union all 
    select * from pending_bl
    union all 
    select * from kpa
    union all 
    select * from gl_actuals
    union all 
    select * from gl_budget
    union all 
    select * from unanet
    union all 
    select 'power_bi_refresh' as data_source
    , convert_timezone('America/Los_Angeles', 'America/New_York', current_timestamp()) as data_date_ref
    , null as first_load_dt
    , convert_timezone('America/Los_Angeles', 'America/New_York', current_timestamp()) as last_load_dt
), metric_union_clean as (
    select 
        data_source,
        case
            when data_source in ('GL Budget', 'New Sales', 'WIP', 'Rolling Forecast', 'Pending Backlog') then last_day(data_date_ref)
            else data_date_ref
        end as data_date_ref,
        last_load_dt
    from metric_union
)

select * from metric_union_clean