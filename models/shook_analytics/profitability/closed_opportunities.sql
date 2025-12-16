with opps as (
    select 
        *
    from 
        {{ ref('int_closed_opportunities')}}
)

select * from opps