with division as (
    select
        "OfficeDivisionId" as office_division_id,
        "OfficeDivisionDescription" as office_division_description,
        "OpportunityId" as opportunity_id 
    from 
        {{ source('unanet', 'office_divisions') }}
)
select * from division