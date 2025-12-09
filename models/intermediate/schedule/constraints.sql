with
    projs as (
        select a.*, b.last_recalc_date
        from {{ source('P6', 'P6_JOB_HEADER') }} as a
        left join {{ source('P6', 'PROJECT') }} as b using (proj_id)
    ),
    task_proj as (
        select p6_full_project_name, last_recalc_date, b.*
        from projs as a
        left join {{ source('P6', 'TASK') }} as b on a.proj_id = b.proj_id
    ),
    consts as (
        select
            proj_id,
            p6_full_project_name as proj_name,
            start_week,
            last_recalc_date,
            count(distinct task_id) as total_tasks_remaining,
            sum(
                case when cstr_type in ('CS_MEO', 'CS_MSO') then 1 else 0 end
            ) as num_hard_consts,
            sum(
                case
                    when cstr_type not in ('CS_MEO', 'CS_MSO') and cstr_type is not null
                    then 1
                    else 0
                end
            ) as num_soft_consts,
            sum(
                case when act_start_date > last_recalc_date then 1 else 0 end
            ) as num_invalid_dates

        from task_proj
        where
            -- start_week >= (select max(START_WEEK) from {{ ref('latest_date') }})
            -- and 
            act_end_date is null
        group by proj_id, p6_full_project_name, start_week, last_recalc_date
    ), const_grades as (
        select 
            proj_id, 
            proj_name,
            start_week,
            num_hard_consts,
            case
                when num_hard_consts < 1 then 'A'
                else 'F' 
            end as hard_consts_grade, 
            num_soft_consts,
            div0(num_soft_consts, total_tasks_remaining) * 100 as pct_soft_consts,
            case
                when pct_soft_consts < 4 then 'A'
                when pct_soft_consts < 5 then 'B'
                when pct_soft_consts < 6 then 'C'
                when pct_soft_consts < 7 then 'D'
                else 'F'
            end as soft_consts_grade,
            num_invalid_dates,
            case
                when num_invalid_dates = 0 then 'A'
                when num_invalid_dates <= 2 then 'B'
                when num_invalid_dates <= 3 then 'c'
                when num_invalid_dates <= 5 then 'D'
                else 'F'
            end as invalid_date_grade
        from 
            consts
    )
select *
from const_grades
where start_week is not null
order by start_week desc, proj_id 