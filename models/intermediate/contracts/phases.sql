select distinct
    trim("JCCo") as JCCO
    , trim("Job") as Job
    , trim("CostType") as cost_type
    , trim("PhaseGroup") as phase_group 
    , trim("Phase") as phase
from 
{{ source('shookdw', 'bjccd') }}
-- group by "JCCo", "Job", "CostType", "PhaseGroup", "Phase"