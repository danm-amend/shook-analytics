with projs as (
    select a.*, b.last_recalc_date 
    from 
        {{ source('P6', 'P6_JOB_HEADER') }} as a 
    left join 
    {{ source('P6', 'PROJECT') }} as b 
    using(proj_id)
), task_proj as (
    select P6_FUll_PROJECT_NAME, last_recalc_date
    , b.*
    FROM projs as a
    left join 
    {{ source('P6', 'TASK') }} as b 
    on a.proj_id = b.proj_id  
), lead_lag as (
    select a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, pred_task_id, task_pred_id as relationship_id
    , lag_hr_cnt
    from task_proj as a 
    left join 
    {{ source('P6', 'TASKPRED') }} as b 
    using(proj_id, task_id)
    -- where start_week = (select max(START_WEEK) from {{ ref('latest_date') }})
    where a.act_end_date is null 
), lead_lag_count as (
    select 
        proj_id, P6_FUll_PROJECT_NAME, start_week
        , count(distinct relationship_id) as remaining_relationships
        , sum(iff(lag_hr_cnt > 0, 1, 0)) as num_lags
        , sum(iff(lag_hr_cnt < 0, 1, 0)) as num_leads
    from 
        lead_lag
    group by proj_id, P6_FUll_PROJECT_NAME, start_week
), lead_lag_pct as (
    select 
        *,
        div0(num_lags, remaining_relationships) * 100 as lags_pct,
        div0(num_leads, remaining_relationships) * 100 as leads_pct
    from 
        lead_lag_count
), lead_lag_grade as (
    select 
        proj_id,
        p6_full_project_name as proj_name,
        start_week,
        remaining_relationships as total_remaining_relationships,
        num_lags as lags_cnt,
        lags_pct,
        case 
            when lags_pct <= 2 then 'A'
            when lags_pct <= 4 then 'B'
            when lags_pct <= 6 then 'C'
            when lags_pct <= 8 then 'D'
            else 'F' 
        end as lags_grade,
        num_leads as leads_cnt,
        leads_pct,
        case 
            when leads_pct = 0 then 'A'
            when leads_pct <= 1 then 'D'
            else 'F'
        end as leads_grade
    from 
        lead_lag_pct 
)
select * from lead_lag_grade
where start_week is not null 
order by start_week desc, proj_id
