with projs as (
    select a.*, b.clndr_id 
    from shookdw.p6.p6_job_header as a 
    left join 
    shookdw.p6.project as b 
    using(proj_id)
), calendar as (
    select a.*, b.day_hr_cnt, b.week_hr_cnt 
        from projs as a 
    left join 
        shookdw.p6.calendar as b 
    on a.clndr_id = b.clndr_id 
), task_proj as (
    select P6_FUll_PROJECT_NAME, day_hr_cnt, week_hr_cnt
    , b.*
    FROM calendar as a
    left join 
    shookdw.p6.task as b 
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
        --         sum(
        --     case 
        --         when (total_float_hr_cnt / day_hr_cnt) < 0 then 1 
        --         else 0 
        --     end 
        -- ) as negative_float,
        sum(total_float_hr_cnt / day_hr_cnt) as negative_float
    from task_proj
    where START_WEEK = '2025-10-26'
    and act_end_date is null
    group by proj_id, p6_full_project_name, day_hr_cnt, week_hr_cnt, start_week
), float_fields as (
    select 
        a.*,
        b."Scheduled_Substantial_Completion_Date" as complete_date,
        b.total_float
    from 
        floats as a 
    left join 
        shookdw.p6.p6_weekly_subtotals as b 
    on a.proj_id = b.proj_id and a.start_week = b.week_ending
), critical_start_end as (
    select					
                        t0.proj_id
                        , min(t1.early_start_date) as critical_path_start
                        , max(t1.late_end_date) as critical_path_end
						-- , SUM(NVL(t2.remain_cost, 0)) as Resources_Remaining_on_Longest_Path
    from					float_fields as t0
    LEFT JOIN               shookdw.p6.TASK as t1
    ON                      t0.proj_id = t1.proj_id
    AND                     t1.START_WEEK = t0.start_week
    AND                     t1.driving_path_flag = 'Y'
    and                     t1.phys_complete_pct < 100
    LEFT JOIN				shookdw.p6.TASKRSRC as t2
    ON						t0.proj_id = t2.proj_id
    AND                     t2.START_WEEK = t0.start_week
    AND                     t1.TASK_ID = t2.TASK_ID
    and                     t2.RSRC_TYPE = 'RT_Labor'
    GROUP BY                t0.proj_id
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