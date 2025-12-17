with opps as (
    select 
        *,
        case
            when stage_type like '%Won%' then true
            else false 
        end as opportunity_won 
    from 
        {{ ref('int_closed_opportunities')}}
)

select * from opps