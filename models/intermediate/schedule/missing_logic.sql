with projs as (
    select a.*, b.last_recalc_date from shookdw.p6.p6_job_header as a 
    left join 
    shookdw.p6.project as b 
    using(proj_id)
), task_proj as (
    select P6_FUll_PROJECT_NAME, last_recalc_date
    , b.*
    FROM projs as a
    left join 
    shookdw.p6.task as b 
    on a.proj_id = b.proj_id  
), pred_task as (
    select a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, a.Task_type, count(distinct pred_task_id) as num_pred_tasks
    from task_proj as a 
    left join 
    shookdw.p6.taskpred as b 
    using(proj_id, task_id)
    where start_week = '2025-10-26'
    group by a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, a.Task_type
), succ_tasks as (
    select 
        a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, a.Task_type, num_pred_tasks, count(distinct b.pred_task_id) as num_suc_tasks
    from pred_task as a 
    left join 
    shookdw.p6.taskpred as b 
    on a.proj_id = b.proj_id and a.task_id = b.pred_task_id
    group by a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, a.Task_type, num_pred_tasks
), missing_logic as (
    select 
        PROJ_Id, P6_FUll_PROJECT_NAME, 
        count(distinct task_id) as total_tasks,
        sum(iff(Task_type in ('TT_Task', 'TT_LOE') and (num_pred_tasks = 0 or num_suc_tasks = 0), 1, 0)) as task_missing_logic,
        sum(iff(Task_type in ('TT_Mile') and (num_suc_tasks = 0), 1, 0)) as start_missing_logic,
        sum(iff(Task_type in ('TT_FinMile') and (num_pred_tasks = 0), 1, 0)) as end_missing_logic
    from 
        succ_tasks
    group by PROJ_Id, P6_FUll_PROJECT_NAME
), missing_logic_pct as (
    select 
        PROJ_Id, P6_FUll_PROJECT_NAME
        , total_tasks
        , task_missing_logic + start_missing_logic + end_missing_logic as total_missing_logic
        , (task_missing_logic + start_missing_logic + end_missing_logic) / total_tasks * 100 as missing_logic_pct
    from
        missing_logic
), missing_logic_grade as (
    select 
        proj_id, 
        p6_full_project_name as proj_name, 
        total_tasks as total_tasks_cnt,
        total_missing_logic as total_missing_logic_cnt ,
        missing_logic_pct, 
        case 
            when total_missing_logic_cnt <= 4 then 'A'
            when total_missing_logic_cnt <= 7 then 'B'
            when total_missing_logic_cnt <= 10 then 'C'
            when total_missing_logic_cnt <= 15 then 'D'
            else 'F'
        end as missing_logic_grade
    from 
        missing_logic_pct
)
select * from missing_logic_grade