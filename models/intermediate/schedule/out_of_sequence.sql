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
    where 1=1 
    -- and b.act_end_date is null 
    and START_WEEK = '2025-10-26'
), pred_task as (
    select a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, a.Task_type, a.act_start_date, a.act_end_date, b.pred_task_id, b.pred_type, b.pred_proj_id  
    -- , count(distinct pred_task_id) as num_pred_tasks
    from task_proj as a 
    left join 
    shookdw.p6.taskpred as b 
    using(proj_id, task_id)
    -- where a.act_end_date is null
    -- group by a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, a.Task_type
), pred_task_details as (
    select 
        a.*,
        b.task_name as pred_task_name,
        b.act_start_date as pred_act_start_date,
        b.act_end_date as pred_act_end_date
    from
        pred_task as a 
    left join 
        task_proj as b 
    on a.proj_id = b.proj_id and a.pred_task_id = b.task_id
    where pred_task_id is not null and pred_task_name is not null 
), pred_task_calc as (
    select 
        *,
        case
            when pred_type = 'PR_FS' and pred_act_end_date is null and act_start_date is not null then 1
            when pred_type = 'PR_SS' and pred_act_start_date is null and act_start_date is not null then 1
            when pred_type = 'PR_SF' and pred_act_start_date is null and act_end_date is not null then 1
            when pred_type = 'PR_FF' and pred_act_end_date is null and act_end_date is not null then 1 
            else 0
        end as out_seq
    from 
        pred_task_details
    where  
        act_end_date is null
), out_seq_cnt as (
    select 
        proj_id, P6_FUll_PROJECT_NAME, START_WEEK, count(distinct pred_task_id) as total_task_cnt, sum(out_seq) as out_seq_cnt
    from 
        pred_task_calc
    group by proj_id, P6_FUll_PROJECT_NAME, start_week
    
), out_seq_grade as (
    select 
        proj_id, 
        p6_full_project_name as proj_name,
        total_task_cnt,
        out_seq_cnt,
        (out_seq_cnt / total_task_cnt) * 100 as out_seq_pct,
        case 
            when out_seq_pct < 1 then 'A'
            when out_seq_pct < 9 then 'B'
            when out_seq_pct < 15 then 'C'
            when out_seq_pct < 20 then 'D'
            else 'F' 
        end as out_seq_grade
    from 
        out_seq_cnt
)
select * from out_seq_grade