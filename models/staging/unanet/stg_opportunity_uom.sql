with uom as (
    select 
        "OpportunityId" as opportunity_id,
        "Unit of Measure" as unit_of_measure,
        last_load_dt 
    from 
        {{ source('unanet', 'opportunity_uom') }}
)

select * from uom