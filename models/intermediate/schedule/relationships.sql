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
), relations as (
    select a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, a.Task_type, b.pred_task_id, b.pred_type, task_pred_id as relationship_id
    from task_proj as a 
    left join 
    shookdw.p6.taskpred as b 
    using(proj_id, task_id)
    where start_week = '2025-11-02'
    and a.act_end_date is null 
    -- group by a.PROJ_Id, a.TASK_ID, a.task_name, a.P6_FULL_PROJECT_NAME, START_WEEK, a.Task_type
), relations_count as (
    select 
        proj_id, P6_FUll_PROJECT_NAME,
        count(distinct relationship_id) as total_remaining_relationships,
        sum(iff(pred_type = 'PR_FS', 1, 0)) as total_fs_relationships
    from 
        relations
    group by proj_id, P6_FUll_PROJECT_NAME
), relations_pct as (
    select 
        *,
        (total_fs_relationships / total_remaining_relationships) * 100 as fs_pct 
    from 
        relations_count
), relations_grade as (
    select 
        proj_id,
        p6_full_project_name as proj_name,
        total_remaining_relationships,
        total_fs_relationships,
        fs_pct,
        case 
            when fs_pct >= 95 then 'A'
            when fs_pct >= 90 then 'B'
            when fs_pct >= 85 then 'C'
            when fs_pct >= 80 then 'D'
            else 'F'
        end as fs_grade
    from relations_pct
)
select * from relations_grade