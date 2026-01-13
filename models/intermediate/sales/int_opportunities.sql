with raw_opps as (
    select 
        *
    from 
        {{ ref("stg_opportunities") }}
    where delete_record = false
    qualify row_number() over (partition by opportunity_number order by last_modified_date_time desc) = 1
), opp_size as (
    select 
        *
    from {{ ref('stg_opportunity_size') }}
    qualify row_number() over (partition by opportunity_id order by last_load_dt desc) = 1
), opp_uom as (
    select 
        *
    from {{ ref('stg_opportunity_uom') }}
    qualify row_number() over (partition by opportunity_id order by last_load_dt desc) = 1
), opp_practice_area as (
    select 
        opportunity_id, last_load_dt
        , listagg(distinct unanet_market_id, ', ') within group (order by unanet_market_id) as market_number
        , listagg(distinct practice_area_name, ', ') within group (order by practice_area_name) as practice_area_name
    from 
        {{ ref('stg_opportunity_practicearea') }} as a 
    left join 
        (
            select distinct unanet_market_id, market_number
            from 
            {{ ref('dim_region_market') }} 
        ) as b
    on a.practice_area_id = b.unanet_market_id
    group by opportunity_id, last_load_dt
    qualify row_number() over (partition by opportunity_id order by last_load_dt desc) = 1

), opp_office_division as (
    select 
        * 
    from
        {{ ref('stg_office_division') }} as a
    left join 
        (
            select distinct unanet_region_id, region_number
            from 
            {{ ref('dim_region_market') }} 
            where region_number not like '%9.%'
        ) as b
    on a.office_division_id = b.unanet_region_id
    qualify row_number() over (partition by opportunity_id order by last_load_dt desc) = 1
), debrief_call_complete as (
    select 
        *
    from 
        {{ ref('stg_opportunity_debrief_call_complete') }}
    qualify row_number() over (partition by opportunity_id order by last_load_dt desc) = 1
)
, opps as (
    select 
        a.*,
        b.estimated_project_size,
        c.unit_of_measure,
        d.market_number,
        d.practice_area_name,
        e.region_number,
        e.office_division_description,
        f.debrief_call_complete
    from 
        raw_opps as a 
    left join 
        opp_size as b
    using(opportunity_id)
    left join 
        opp_uom as c
    using(opportunity_id)
    left join 
        opp_practice_area as d 
    using(opportunity_id) 
    left join 
        opp_office_division as e 
    using(opportunity_id)
    left join 
        debrief_call_complete as f 
    using(opportunity_id)
), opps_cols as (
    select 
        opportunity_id,
        client_id,
        client_name, 
        opportunity_name, 
        cost,
        coalesce(estimated_project_size, size) as size,
        unit_of_measure,
        probability,
        stage,
        stage_type,
        market_number,
        practice_area_name as market_channel,
        region_number,
        office_division_description as region,
        debrief_call_complete,
        regexp_replace(regexp_replace(next_action, '<[^>]+>', ''), '&nbsp;', '') as next_action,
        regexp_replace(regexp_replace(note, '<[^>]+>', ''), '&nbsp;', '') as note,
        opportunity_number,
        address1,
        city,
        state,
        postal_code,
        country,
        construction_start_date,
        construction_completion_date,
        create_date_time,
        last_modified_date_time
    from opps
)

select *
from opps_cols