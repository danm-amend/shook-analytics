with proj_tasks as (
    select 
        * 
    from
        {{ source('P6', 'TASK') }} 
), ofg as (
    select proj_id, task_id, start_week, short_name
    from 
        {{ source('P6', 'TASKACTV') }} AS a
    left join
        {{ source('P6', 'ACTVCODE') }} as b
    using (actv_code_id, actv_code_type_id)
    where short_name = 'OFG'
    group by proj_id, task_id, start_week, short_name 
), proj_tasks_omitted as (
        select 
        * 
    from 
        proj_tasks as a 
    left join 
        ofg as b 
    using (proj_id, task_id, start_week)
    where b.short_name is null
)
select * from proj_tasks_omitted