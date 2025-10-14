with pending_backlog as (
    select 
        *
    from 
        {{ ref('pending_backlog') }}
)

select * from pending_backlog