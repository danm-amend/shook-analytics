with closed_opps as (
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
        replace(replace(next_action, '<p>', ''), '</p>', '') as next_action,
        replace(replace(note, '<p>', ''), '</p>', '') as note,
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
    -- where stage_type not in ('Open', 'Pending') 
    where stage_type like 'Closed%'
    and delete_record = false
), region_market as (
    select 
        a.*,
        b."Market Channels" as market_channel,
        b."Office Division" as office_division
    from 
        closed_opps as a 
    left join 
        {{ source('unanet', 'opportunity_export') }} as b 
    on trim(opportunity_number) = trim("Opp Number")
)

select * from region_market