with practiceareas as (
    select 
        "OpportunityId" as opportunity_id, 
        practiceareaid as practice_area_id,
        practiceareaname as practice_area_name,
        last_modified_dt as last_load_dt
    from 
        {{ source('unanet', 'opportunity_practicearea') }}
)

select * from practiceareas