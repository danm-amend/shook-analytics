with projs as (
    select a.*, b.clndr_id 
    from 
    {{ source('P6', 'P6_JOB_HEADER') }} as a 
    left join 
    {{ source('P6', 'PROJECT') }} as b 
    using(proj_id)
), calendar as (
    select a.*, b.day_hr_cnt, b.week_hr_cnt 
        from projs as a 
    left join 
        {{ source('P6', 'CALENDAR') }} as b 
    on a.clndr_id = b.clndr_id 
), baseline as (
    select 
        *
    from 
    {{ source('P6', 'PROJECT') }}
    where orig_proj_id is not null and last_baseline_update_date is not null
    qualify rank() over (partition by orig_proj_id order by last_baseline_update_date desc, proj_id desc) = 1
), task_proj as (
    select P6_FUll_PROJECT_NAME, day_hr_cnt, week_hr_cnt, start_week, act_start_date
    , b.proj_id, b.task_code, b.task_id, target_drtn_hr_cnt
    FROM calendar as a
    left join 
    {{ source('P6', 'TASK') }} as b 
    on a.proj_id = b.proj_id  
    -- WHERE B.start_week = (select max(START_WEEK) from {{ ref('latest_date') }}) 
    where act_end_date is null
), baseline_join as (
    select 
        a.proj_id, 
        a.task_code,
        b.orig_proj_id,
        a.p6_full_project_name,
        a.day_hr_cnt,
        a.start_week,
        b.proj_id as baseline_proj_id,
        c.target_drtn_hr_cnt
    from 
        task_proj as a 
    left join 
        baseline as b 
    on a.proj_id = b.orig_proj_id 
    left join 
        {{ source('P6', 'TASK') }} as c 
    on b.proj_id = c.proj_id and a.task_code = c.task_code
), proj_agg as (
    select 
        proj_id,
        P6_FUll_PROJECT_NAME,
        baseline_proj_id,
        start_week,
        count(*) as num_tasks,
        sum(iff(target_drtn_hr_cnt / day_hr_cnt > 20, 1, 0)) as long_duration
    from baseline_join 
    group by
        proj_id,
        P6_FUll_PROJECT_NAME,
        baseline_proj_id,
        start_week        
), dur_grade as (
    select 
        proj_id, 
        p6_full_project_name as proj_name,
        start_week,
        baseline_proj_id, 
        num_tasks as remaining_tasks_cnt,
        long_duration as long_duration_cnt,
        (long_duration / num_tasks) * 100 as long_dur_pct,
        case 
            when long_dur_pct <= 1 then 'A'
            when long_dur_pct <= 2 then 'B'
            when long_dur_pct <= 3 then 'C'
            when long_dur_pct <= 4 then 'D'
            else 'F'
        end as dur_grade

    from 
        proj_agg
)

select * from  dur_grade
order by start_week desc, proj_id