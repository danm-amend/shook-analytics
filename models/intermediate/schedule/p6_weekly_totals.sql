with latest_date as (
    select MAX(start_week) as START_WEEK
    from shookdw.p6.task
)

select * from latest_date