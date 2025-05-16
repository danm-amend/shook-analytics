with job_desc as (
    select JCCo, Job, CONTRACT, DESCRIPTION, START_DATE, ACTUAL_CLOSE_DATE 
    from
    {{ ref('contract_master') }}
), bjccd_formatted as (
    select 
        "JCCo" as JCCO, 
        "Job" as JOB,
        *
    from {{ source('shookdw', 'bjccd') }}
), orig_est as (
    select 
    JCCO
    , JOB
    , sum("EstCost") as orig_estimate
    , max("ActualDate") as orig_est_max_date
    from bjccd_formatted
    where 1=1 
    and "JCTransType" = 'OE' 
    group by JCCo, Job 
), current_est as (
    select 
        JCCo
        , Job
        , sum("EstCost") as curr_estimate
        , max("ActualDate") as curr_est_max_date
    from bjccd_formatted
    where 1=1 
    and "JCTransType" in ('OE', 'CO') 
    group by JCCo, Job
), proj_est as (
    select 
        JCCo
        , Job
        , sum("ProjCost") as proj_estimate
        , max("ActualDate") as proj_est_max_date
    from bjccd_formatted
    where 1=1 
    and "JCTransType" = 'PF' 
    group by JCCo, Job
), cost_to_date as (
    select 
        JCCo,
        Job, 
        sum("ActualCost") as cost_to_date,
        sum("TotalCmtdCost") as committed_cost,
        max("ActualDate") as cost_max_date
    from 
    bjccd_formatted
    group by JCCo, Job
), proj_comp as (
    select 
        a.*
        , b.orig_estimate
        , b.orig_est_max_date
        , c.curr_estimate
        , c.curr_est_max_date
        , d.proj_estimate
        , d.proj_est_max_date
        , e.cost_to_date
        , e.committed_cost
        , e.cost_max_date
        , round((e.cost_to_date / proj_estimate) * 100, 2) as percent_complete
        , CASE 
            when orig_estimate = 0 then 'zero est'
            when proj_estimate > orig_estimate then 'over'
            when proj_estimate = orig_estimate then 'same'
            when proj_estimate < orig_estimate then 'under'
            else null
        END as comp_orig_est
        , CASE 
            when curr_estimate = 0 then 'zero est'
            when proj_estimate > curr_estimate then 'over'
            when proj_estimate = curr_estimate then 'same'
            when proj_estimate < curr_estimate then 'under'
            else null
        END as comp_curr_est
    from
        job_desc as a 
    left join orig_est as b 
    using(JCCO, JOB)
    left join current_est as c 
    using(JCCO, JOB)
    left join proj_est as d 
    using(JCCO, JOB)
    left join cost_to_date as e 
    using(JCCO, JOB)
) 
select 
    *
from proj_comp
where START_DATE >= '2023-01-01'
and (percent_complete >= 98 or ACTUAL_CLOSE_DATE is not null)
and cost_to_date > 0 and percent_complete <= 100


