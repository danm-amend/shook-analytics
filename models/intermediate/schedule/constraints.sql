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
)
, consts as ( 
    select 
        proj_id,
        p6_full_project_name as proj_name, 
        last_recalc_date,
        count(distinct task_id) as total_tasks_remiaining,
        sum(
            case
                when CSTR_TYPE in ('CS_MEO', 'CS_MSO') then 1
                ELSE 0
            end
        ) as num_hard_consts,
        sum(
            case
                when CSTR_TYPE not in ('CS_MEO', 'CS_MSO') and CSTR_TYPE is not null then 1
                ELSE 0
            end
        ) as num_soft_consts,
        sum(
            case
                when act_start_date > last_recalc_date then 1
                else 0
            end 
        ) as invalid_dates

    from task_proj
    where START_WEEK >= CAST(DATEADD(day, 0 - DAYOFWEEK(CURRENT_DATE()), CURRENT_DATE()) AS TIMESTAMP)
    and act_end_date is null
    group by proj_id, p6_full_project_name, last_recalc_date
)
select * from consts