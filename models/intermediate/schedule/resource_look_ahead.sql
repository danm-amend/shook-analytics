with projs as (
    select 
        PROJ_ID as p6_proj_id,
        project_name as viewpoint_proj_id,
        P6_full_project_name
    from shookdw.p6.p6_job_header 
), field_labor as (
    select 
        a.*,
        -- sum("ActualCost") as total_labor_cost
        -- "ActualCost"
        sum(iff(b."ActualDate" between dateadd(DAY, 1, DATEADD(WEEK, -6, (select max(start_week) from intermediate.schedule.latest_date)))
            AND (select max(start_week) from intermediate.schedule.latest_date), "ActualCost", 0)) as total_labor_cost,
        sum(iff(b."ActualDate" between dateadd(DAY, 1, DATEADD(WEEK, -1, (select max(start_week) from intermediate.schedule.latest_date)))
            AND (select max(start_week) from intermediate.schedule.latest_date), "ActualCost", 0)) as current_labor_cost
    from projs as a
    left join 
    shookdw.viewpoint.bjccd as b 
    on a.viewpoint_proj_id = split_part(b."Job", '.', 1)  
    -- where b."ActualDate" between dateadd(DAY, 1, DATEADD(WEEK, -6, (select max(start_week) from intermediate.schedule.latest_date)))
    -- AND (select max(start_week) from intermediate.schedule.latest_date)
    and b."JCCo" = 1
    and "CostType" = 1 and LEFT("Phase", 2) not in ('01', '97', '96', '98')
    group by p6_proj_id, viewpoint_proj_id, P6_full_project_name
), resources_used as (
    select proj_id, 
        sum(iff(week_ending = DATEADD(WEEK, -6, (select max(start_week) from intermediate.schedule.latest_date)), "Resources_Completed_To_Date", 0)) as resource_6wa,
        sum(iff(week_ending = DATEADD(WEEK, -1, (select max(start_week) from intermediate.schedule.latest_date)), "Resources_Completed_To_Date", 0)) as resource_1wa,
        sum(iff(week_ending = (select max(start_week) from intermediate.schedule.latest_date), "Resources_Completed_To_Date", 0)) as resource_end,
    from shookdw.p6.p6_weekly_subtotals
    group by proj_id
), resources_cost as (
    select 
        a.*,
        b.resource_end - b.resource_6wa as resources_used_6_weeks,
    from field_labor as a 
    left join 
    resources_used as b 
    on a.p6_proj_id = b.proj_id
)
, cpi as (
    select 
        *,
        div0(resources_used_6_weeks, total_labor_cost) as six_week_cpi 
    from resources_cost
), next_week_resources as (
    select 
        a.*,
        b."Next_Week_Resources" as next_week_resources
    from cpi as a 
    left join 
    shookdw.p6.p6_weekly_subtotals as b
    on a.p6_proj_id = b.proj_id
    where b.week_ending = (select max(start_week) from intermediate.schedule.latest_date)
), resource_look_ahead as (
    select 
        p6_proj_id as proj_id,
        P6_full_project_name,
        six_week_cpi,
        current_labor_cost,
        next_week_resources,
        div0((current_labor_cost * six_week_cpi), next_week_resources) * 100 as resource_look_ahead_pct
    from next_week_resources
    
), resource_look_ahead_grade as (
    select 
        proj_id,
        p6_full_project_name as proj_name,
        six_week_cpi,
        current_labor_cost,
        next_week_resources,
        resource_look_ahead_pct,
        case 
            when resource_look_ahead_pct between 90 and 120 then 'A'
            when resource_look_ahead_pct between 85 and 90 or resource_look_ahead_pct between 120 and 130 then 'B'
            when resource_look_ahead_pct between 80 and 85 or resource_look_ahead_pct between 130 and 140 then 'C'
            when resource_look_ahead_pct between 75 and 80 or resource_look_ahead_pct between 140 and 150 then 'D'
            else 'F'
        end as resource_look_ahead_grade
    from 
        resource_look_ahead
)
select * from resource_look_ahead_grade