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
        first_load_dt,
        last_load_dt
    from 
        {{ ref('osha_recordable_incidents') }}
    CROSS JOIN 
    (
        select min(last_load_dt) as first_load_dt, max(last_load_dt) as last_load_dt from {{ source('metadata', 'kpa_refresh_log') }} where success = true
    )
    qualify row_number() over (partition by null order by injury_date desc) = 1
), gl_actuals as (

    SELECT TABLE_NAME, LAST_ALTERED
    FROM shookdw.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'VIEWPOINT'
    and table_name in ('GLPI', 'GLAC', 'GLAS', 'BJCDM')
), gl_actuals_refresh as (
    select 
        'GL Actuals' as data_source,
        LAST_ALTERED as data_date_ref,
        null as first_load_dt,
        LAST_ALTERED as last_load_dt
    from 
        gl_actuals
    qualify row_number() over (partition by null order by LAST_ALTERED asc) = 1
), gl_budget as (
    SELECT TABLE_NAME, LAST_ALTERED
    FROM shookdw.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'VIEWPOINT'
    and table_name in ('GLAC', 'GLBD', 'BJCDM', 'GLBC')
), gl_budget_refresh as (
    select 
        'GL Budget' as data_source,
        LAST_ALTERED as data_date_ref,
        null as first_load_dt,
        LAST_ALTERED as last_load_dt
    from 
        gl_budget
    qualify row_number() over (partition by null order by LAST_ALTERED asc) = 1
)
, metric_union as (
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
    select * from gl_actuals_refresh
    union all 
    select * from gl_budget_refresh
), metric_union_clean as (
    select 
        data_source,
        date(convert_timezone('America/Los_Angeles', 'America/New_York', data_date_ref)) as data_date_ref,
        convert_timezone('America/Los_Angeles', 'America/New_York', last_load_dt) as last_load_dt
    from metric_union
)

select * from metric_union_clean