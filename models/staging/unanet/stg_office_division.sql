with division as (
    select
        "OfficeDivisionId" as office_division_id,
        "OfficeDivisionDescription" as office_division_description,
        "OpportunityId" as opportunity_id,
        last_modified_dt as last_load_dt
    from 
        {{ source('unanet', 'office_divisions') }}
)
select * from division