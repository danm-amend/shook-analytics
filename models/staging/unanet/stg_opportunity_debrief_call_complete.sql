with debrief as (
    select 
        "OpportunityId" as opportunity_id, 
        to_boolean("Debrief Call Complete?") as debrief_call_complete,
        last_load_dt
    from 
        {{ source('unanet', 'opportunity_debrief_call_complete') }}
)

select * from debrief 