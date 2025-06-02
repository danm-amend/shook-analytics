select 
    "JCCo" as JCCO
    , "Job" as Job
    , "CostType" as cost_type
    , "PhaseGroup" as phase_group 
    , "Phase" as phase
from 
{{ source('shookdw', 'bjccd') }}
group by "JCCo", "Job", "CostType", "PhaseGroup", "Phase"