with committed_cost as (
    select 
        trim("JCCo") as JCCO,
        trim("Job") as Job,
        "CostTrans" as cost_trans,
        "Mth" as mth,
        "ActualDate" as actual_date,
        "JCTransType" as trans_type,
        "PhaseGroup" as phase_group,
        "Phase" as phase,
        "CostType" as cost_type,
        "TotalCmtdUnits" as committed_units,
        "TotalCmtdCost" as committed_cost,
        "RemainCmtdUnits" as remain_committed_units,
        "RemainCmtdCost" as remain_committed_cost
    from 
    {{ source('shookdw', 'bjccd') }}
    where "TotalCmtdCost" != 0
) 

select * from committed_cost