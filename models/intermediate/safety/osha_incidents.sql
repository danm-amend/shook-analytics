with osha_incidents as (
    select 
        datetime as submit_datetime,
        Injury_Date,
        job_title,
        Field_Office,
        Injury_Type,
        Periods_Away_restricted,
        to_boolean(select_Days_Away_From_Work) as select_Days_Away_From_Work,
        to_boolean(select_Job_Transfer_or_Restriction) as select_Job_Transfer_or_Restriction,
        to_boolean(select_Death) as select_Death,
        to_boolean(select_Other_Recordable_Case) select_Other_Recordable_Case,
        outcome_2 as injury_outcome,

    from 
        {{ ref('stg_osha_recordable_incidents') }}
), incident_type as (
    select 
        *,
        case 
            when select_Days_Away_From_Work = true then 'Days Away Incident'
            when select_Job_Transfer_or_Restriction = true then 'Job Transfer Incident'
            when select_Death = true then 'Death Incident'
            else 'Other Recordable Incident'
        end as incident_type
    from 
        osha_incidents
)

select * from incident_type