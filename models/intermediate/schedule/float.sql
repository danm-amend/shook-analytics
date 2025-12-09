with projs as (
    select a.*, b.clndr_id 
    from {{ source('P6', 'P6_JOB_HEADER') }} as a 
    left join 
    {{ source('P6', 'PROJECT') }} as b 
    using(proj_id)
), calendar as (
    select a.*, b.day_hr_cnt, b.week_hr_cnt 
        from projs as a 
    left join 
        {{ source('P6', 'CALENDAR') }} as b 
    on a.clndr_id = b.clndr_id 
), task_proj as (
    select P6_FUll_PROJECT_NAME, day_hr_cnt, week_hr_cnt
    , b.*
    FROM calendar as a
    left join 
    {{ source('P6', 'TASK') }} as b 
    on a.proj_id = b.proj_id  
), floats as ( 
    select 
        proj_id,
        p6_full_project_name as proj_name, 
        day_hr_cnt,
        week_hr_cnt,
        start_week,
        count(distinct task_id) as total_tasks_remaining,
        sum(
            case 
                when (total_float_hr_cnt / day_hr_cnt) > 44 then 1 
                else 0 
            end 
        ) as high_float,
        sum(total_float_hr_cnt / day_hr_cnt) as negative_float
    from task_proj
    -- where START_WEEK = (select max(START_WEEK) from {{ ref('latest_date') }})
    where act_end_date is null
    group by proj_id, p6_full_project_name, day_hr_cnt, week_hr_cnt, start_week
), float_fields as (
    select 
        a.*,
        c.Contractural_Substantial_Completion as complete_date,
        b.total_float
    from 
        floats as a 
    left join 
        {{ source('P6', 'P6_WEEKLY_SUBTOTALS') }} as b 
    on a.proj_id = b.proj_id and a.start_week = b.week_ending
    left join 
        {{ source('P6', 'P6_JOB_HEADER') }} as c 
    on a.proj_id = trim(c.proj_id)
), critical_start_end as (
    select					
                        t1.proj_id
                        , t1.start_week as critical_path_start
                        , t1.early_end_date as critical_path_end
                        , t1.task_name
                        , t1.task_type
                        , pw.wbs_name
    from					float_fields as t0
    LEFT JOIN               {{ source('P6', 'TASK') }} as t1
    ON                      t0.proj_id = t1.proj_id
    AND                     t1.START_WEEK = t0.start_week
    left join 
    {{ source('P6', 'PROJWBS') }} as pw 
    on pw.proj_id = t1.proj_id and t1.wbs_id = pw.wbs_id 
    where 1=1 
    and wbs_name in ('Milestones')
    and task_type in ('TT_FinMile')
    and t1.start_week = (select max(START_WEEK) from {{ ref('latest_date') }}) 
    and t1.act_end_date is null 
    qualify row_number() over (partition by t0.proj_id order by t1.early_end_date asc) = 1
), float_duration_calc as (
    select 
        *,
        round(datediff(day, start_week, complete_date) / 7, 0) as weeks_until_finish,
        datediff(day, start_week, complete_date) % 7 as remaining_days, 
        week_hr_cnt / day_hr_cnt as days_per_week,
        round(datediff(day, critical_path_start, critical_path_end) / 7, 0) as crit_path_dur_weeks,
        datediff(day, critical_path_start, critical_path_end) % 7 as remain_crit_path_days
    from 
        float_fields
    left join 
        critical_start_end 
    using(proj_id)
), remaining_proj_days as (
    select 
        *,
        (weeks_until_finish * days_per_week) + remaining_days as remaining_working_days,
        (crit_path_dur_weeks * days_per_week) + remain_crit_path_days as critical_path_duration
    from float_duration_calc 
), float_cons_idx as (
    select 
        *
        , div0((remaining_working_days + total_float), remaining_working_days) as tfci
        , div0((critical_path_duration + total_float), critical_path_duration) as cpli
    from 
        remaining_proj_days
), float_grade as (
    select 
        proj_id, 
        proj_name,
        start_week,
        high_float,
        (high_float/total_tasks_remaining) * 100 as pct_high_float,
        case    
            when pct_high_float < 3 then 'A'
            when pct_high_float < 5 then 'B'
            when pct_high_float < 10 then 'C'
            when pct_high_float < 15 then 'D'
            else 'F'
        end as high_float_grade,
        -- negative_float,
        total_float * 8 / day_hr_cnt as total_float_days,
        case 
            when total_float_days > 15 then 'A'
            when total_float_days between 0 and 15 then 'B'
            when total_float_days between -10 and -1 then 'C'
            when total_float_days between -20 and -11 then 'D'
            else 'F'
        end as negative_float_grade,
        TFCI,
        case
            when tfci >= 1 then 'A'
            when tfci >= .97 then 'B'
            when tfci >= .95 then 'C'
            when tfci >= .9 then 'D'
            else 'F'
        end as tfci_grade,
        CPLI,
        case
            when cpli >= 1 then 'A'
            when cpli >= .95 then 'B'
            when cpli >= .9 then 'C'
            when cpli >= .85 then 'D'
            else 'F'
        end as cpli_grade
    from
        float_cons_idx  
)
-- select * from float_cons_idx
select * from float_grade
order by start_week desc, proj_id
-- select * from task_proj