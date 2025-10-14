
with projs as (
    select a.*, b.clndr_id from {{ source('P6', 'P6_JOB_HEADER') }} as a 
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
    select P6_FUll_PROJECT_NAME, day_hr_cnt
    , b.*
    FROM calendar as a
    left join 
    {{ source('P6', 'TASK') }} as b 
    on a.proj_id = b.proj_id  
), floats as ( 
    select 
        proj_id,
        p6_full_project_name as proj_name, 
        count(distinct task_id) as total_tasks_remiaining,
        sum(
            case 
                when (total_float_hr_cnt / day_hr_cnt) > 44 then 1 
                else 0 
            end 
        ) as high_float,
                sum(
            case 
                when (total_float_hr_cnt / day_hr_cnt) < 0 then 1 
                else 0 
            end 
        ) as negative_float
    from task_proj
    where start_week >= (select max(START_WEEK) from {{ ref('latest_date') }})
    and act_end_date is null
    group by proj_id, p6_full_project_name
)
select * from floats
