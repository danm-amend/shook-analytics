with opps as (
    select 
        *,
        case
            when stage_type like '%Won%' then true
            else false 
        end as opportunity_won 
    from 
        {{ ref('int_opportunities')}}
    where stage_type like 'Closed%'
)

select * from opps