with projs as (
    select 
        PROJ_ID,
        P6_full_project_name
    from shookdw.p6.p6_job_header 
), baseline as (
    select 
        *
    from shookdw.p6.project
    where orig_proj_id is not null and last_baseline_update_date is not null
    qualify rank() over (partition by orig_proj_id order by last_baseline_update_date desc, proj_id desc) = 1
), proj_baseline as (
    select 
        a.*,
        b.proj_id as baseline_proj_id,
        b.last_baseline_update_date
    from projs as a 
        left join 
    baseline as b 
        on a.proj_id = b.orig_proj_id 
    
), actual_complete_count as (
    select 
        a.proj_id,
        count(distinct task_id) as total_tasks,
        sum(iff(act_end_date is not null, 1, 0)) as actual_complete
    from proj_baseline as a 
    left join 
    shookdw.p6.task as b
    using(proj_id)
    where b.start_week = '2025-10-26'
    group by a.PROJ_ID
    
), baseline_complete_count as (
    select 
        a.proj_id
        , a.baseline_proj_id
        , sum(iff(b.target_end_date <= '2025-10-26', 1, 0)) as baseline_complete
    from proj_baseline as a 
    left join 
    shookdw.p6.task as b 
    on a.baseline_proj_id = b.proj_id
    -- where a.proj_id = '21837'
    where b.start_week = '2025-10-26'
    group by a.proj_id, baseline_proj_id
)
, both_counts as (
    select 
        a.*, 
        b.total_tasks,
        b.actual_complete,
        c.baseline_complete
    from proj_baseline as a
    left join 
    actual_complete_count as b
    using(proj_id)
    left join 
    baseline_complete_count as c 
    using(proj_id)
), missed_tasks as (
    select 
        a.PROJ_ID,
        a.baseline_proj_id,
        b.act_end_date,
        b.target_end_date,
        b.early_end_date,
        b.late_end_date,
        c.act_end_date as bs_act_end,
        c.target_end_date as bs_target,
        c.early_end_date as bs_early_end,
        c.late_end_date as bs_late_end
    from 
        both_counts as a 
    left join 
        shookdw.p6.task as b
    using(proj_id)
    left join 
        shookdw.p6.task as c
    on a.baseline_proj_id = c.proj_id and b.task_code = c.task_code
    where b.start_week = '2025-10-26' 
    and c.start_week = '2025-10-26'
    -- and a.proj_id = '21837'
), missed_tasks_counts as (
    select 
        proj_Id, 
        baseline_proj_id,
        sum(iff(act_end_date > bs_target
            or (act_end_date is null and bs_target <= '2025-10-26')
            , 1, 0)) as num_missed_tasks,
        sum(iff(bs_target <= '2025-10-26', 1, 0)) as num_finished_baseline
    from
        missed_tasks
    group by proj_id, baseline_proj_id
), both_counts_missed_tasks as (
    select 
        a.*, 
        b.num_missed_tasks,
        b.num_finished_baseline
    from both_counts as a 
    left join 
    missed_tasks_counts as b
    using(proj_id)
), baseline_grade as (
    select 
        proj_id,
        p6_full_project_name as proj_name,
        baseline_proj_id,
        total_tasks as total_tasks_cnt,
        actual_complete,
        baseline_complete,
        actual_complete / baseline_complete as bei,
        case 
            when bei >= 1 then 'A'
            when bei >= .95 then 'B'
            when bei >= .9 then 'C'
            when bei >= .85 then 'D'
            else 'F' 
        end as bei_grade,
        num_Missed_tasks,
        (num_missed_tasks / baseline_complete) * 100 as missed_pct,
        case 
            when missed_pct >= 100 then 'A'
            when missed_pct >= 95 then 'B'
            when missed_pct >= 90 then 'C'
            when missed_pct >= 85 then 'D'
            else 'F'
        end as miss_act_grade 
    from 
        both_counts_missed_tasks
)
-- select * from both_counts_missed_tasks
select * from baseline_grade