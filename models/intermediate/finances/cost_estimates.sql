select 
    trim("JCCo") as JCCO,
    trim("Job") as Job,
    "CostTrans" as cost_trans,
    "Mth" as Mth,
    "ActualDate" as actual_date,
    "JCTransType" as trans_type,
    "PhaseGroup" as phase_group,
    trim("Phase") as phase,
    "CostType" as cost_type,
    case 
    when "JCTransType" in ('OE', 'CO') then "EstCost"
    WHEN "JCTransType" = 'PF' then "ProjCost" 
    else 0
    end as estimated_cost
from {{ source('shookdw', 'bjccd') }}
where trim("JCTransType") in ('OE', 'CO', 'PF')
-- and trim("Job") = '123009.'
-- and Phase like '80%'
