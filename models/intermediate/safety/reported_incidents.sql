with incidents as (
    select 
        * 
    from
        {{ ref('stg_incident_notification') }}
    where 1=1
    -- and datetime >= '2025-03-22'
    and employee_supervisor != 'Rookie Shookie'
    and observer != 'Rookie Shookie'
    and employee_involved not in ('Me', 'Rachel Hamm')
    order by datetime_of_incident desc
)

select Region, Project, incident_type, datetime_of_incident, 
from incidents
