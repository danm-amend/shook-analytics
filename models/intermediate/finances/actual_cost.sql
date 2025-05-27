with actual_cost as (
    select 
        trim("JCCo") as JCCO,
        trim("Job") as Job,
        "CostTrans" as cost_trans,
        "Mth" as Mth, 
        "ActualDate" as actual_date,
        "JCTransType" as trans_type,
        "PhaseGroup" as phase_group,
        "Phase" as phase,
        "CostType" as cost_type,
        "ActualCost" as actual_cost
    from 
    {{ source('shookdw', 'bjccd') }}
    where "ActualCost" != 0
) 

select * from actual_cost