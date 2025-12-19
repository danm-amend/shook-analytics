with open_opps as (
    select 
        opportunity_id,
        client_id,
        client_name, 
        opportunity_name, 
        cost,
        size,
        probability,
        stage,
        stage_type,
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
        last_modified_date_time,
        
    from 
        {{ ref("stg_opportunities") }}
    where stage_type in ('Open', 'Pending')
    and delete_record = false
), region_market as (
    select 
        a.*,
        b."Market Channels" as market_channel,
        b."Office Division" as office_division
    from 
        open_opps as a 
    left join 
        {{ source('unanet', 'opportunity_export') }} as b 
    on trim(opportunity_number) = trim("Opp Number")
)

select * from region_market