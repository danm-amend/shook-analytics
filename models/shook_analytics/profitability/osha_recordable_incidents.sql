select 
    submit_datetime,
    Injury_Date,
    DATEFROMPARTS(YEAR(CAST(Injury_Date AS DATE)), MONTH(CAST(Injury_Date AS DATE)), 1) AS Injury_Month,
    job_title,
    Field_Office,
    incident_type,
    ifnull(Periods_Away_Restricted, 0) as Periods_Away_or_restricted
from
    {{ ref('osha_incidents') }}