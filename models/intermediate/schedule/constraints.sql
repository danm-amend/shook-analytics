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
            last_recalc_date,
            count(distinct task_id) as total_tasks_remiaining,
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
            ) as invalid_dates

        from task_proj
        where
            start_week >= (select max(START_WEEK) from {{ ref('latest_date') }})
            and act_end_date is null
        group by proj_id, p6_full_project_name, last_recalc_date
    )
select *
from consts
