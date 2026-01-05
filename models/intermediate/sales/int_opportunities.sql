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
        *
    from 
        {{ ref('stg_opportunity_practicearea') }}
    qualify row_number() over (partition by opportunity_id order by last_load_dt desc) = 1
), opp_office_division as (
    select 
        * 
    from
        {{ ref('stg_office_division') }}
     qualify row_number() over (partition by opportunity_id order by last_load_dt desc) = 1
), debrief_call_complete as (
    select 
        *
    from 
        {{ ref('stg_opportunity_debrief_call_complete') }}
    qualify row_number() over (partition by opportunity_id order by last_load_dt desc) = 1
), opps as (
    select 
        a.*,
        b.estimated_project_size,
        c.unit_of_measure,
        d.practice_area_name,
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
        -- size,
        coalesce(estimated_project_size, size) as size,
        unit_of_measure,
        probability,
        stage,
        stage_type,
        practice_area_name as market_channel,
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