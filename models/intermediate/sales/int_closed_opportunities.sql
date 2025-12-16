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
)

select * from open_opps