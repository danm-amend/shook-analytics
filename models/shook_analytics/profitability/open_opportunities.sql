with opps as (
    select 
        *
    from 
        {{ ref('int_open_opportunities')}}
)

select * from opps