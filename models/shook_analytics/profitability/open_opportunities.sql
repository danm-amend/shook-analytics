with opps as (
    select 
        *
    from 
        {{ ref('int_opportunities')}}
    where stage_type in ('Open', 'Pending')
)

select * from opps