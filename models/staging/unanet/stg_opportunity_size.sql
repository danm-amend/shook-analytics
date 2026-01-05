with opp_size as (
    select 
        "OpportunityId" as opportunity_id,
        cast("Estimated Project Size" as number) as estimated_project_size,
        last_load_dt
    from 
        {{ source('unanet', 'opportunity_size') }}
)

select * from opp_size