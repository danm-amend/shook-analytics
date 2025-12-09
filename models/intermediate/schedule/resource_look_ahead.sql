with projs as (
    select 
        PROJ_ID as p6_proj_id,
        project_name as viewpoint_proj_id,
        P6_full_project_name
    from 
    {{ source('P6', 'P6_JOB_HEADER') }}
), start_weeks as (
    select 
        distinct start_week, proj_id as p6_proj_id 
    from 
        {{ source('P6', 'TASK')}}
), proj_start_weeks as (
    select 
        *
    from 
        projs as a 
    left join 
        start_weeks as b 
    using(p6_PROJ_ID)
), field_labor as (
    select 
        a.*,
        -- sum("ActualCost") as total_labor_cost
        -- "ActualCost"
        -- sum(iff(b."ActualDate" between dateadd(DAY, 1, DATEADD(WEEK, -6, (select max(start_week) from intermediate.schedule.latest_date)))
        --     AND (select max(start_week) from intermediate.schedule.latest_date), "ActualCost", 0)) as total_labor_cost,
        sum(iff(b."ActualDate" between dateadd(DAY, 1, DATEADD(WEEK, -6, start_week))
            AND start_week, "ActualCost", 0)) as total_labor_cost,
        -- sum(iff(b."ActualDate" between dateadd(DAY, 1, DATEADD(WEEK, -1, (select max(start_week) from intermediate.schedule.latest_date)))
        --     AND (select max(start_week) from intermediate.schedule.latest_date), "ActualCost", 0)) as current_labor_cost,
        sum(iff(b."ActualDate" between dateadd(DAY, 1, DATEADD(WEEK, -1, start_week))
            AND start_week, "ActualCost", 0)) as current_labor_cost
    from proj_start_weeks as a
    left join 
    {{ source('shookdw', 'bjccd') }} as b 
    on a.viewpoint_proj_id = split_part(b."Job", '.', 1)  
    -- where b."ActualDate" between dateadd(DAY, 1, DATEADD(WEEK, -6, (select max(start_week) from intermediate.schedule.latest_date)))
    -- AND (select max(start_week) from intermediate.schedule.latest_date)
    and b."JCCo" = 1
    and "CostType" = 1 and LEFT("Phase", 2) not in ('01', '97', '96', '98')
    group by p6_proj_id, viewpoint_proj_id, P6_full_project_name, start_week
), resources_used as (
    select proj_id, week_ending,
        -- sum(iff(week_ending = DATEADD(WEEK, -6, (select max(start_week) from intermediate.schedule.latest_date)), "Resources_Completed_To_Date", 0)) as resource_6wa,
        -- sum(iff(week_ending = DATEADD(WEEK, -6, week_ending), "Resources_Completed_To_Date", 0)) as resource_6wa,
        LEAD("Resources_Completed_To_Date", 6) OVER (PARTITION BY PROJ_ID ORDER by week_ending desc) as resource_6wa,
        LEAD("Resources_Completed_To_Date", 1) OVER (PARTITION BY PROJ_ID ORDER by week_ending desc) as resource_1wa,
        -- sum(iff(week_ending = DATEADD(WEEK, -1, (select max(start_week) from intermediate.schedule.latest_date)), "Resources_Completed_To_Date", 0)) as resource_1wa,
        -- sum(iff(week_ending = DATEADD(WEEK, -1, week_ending), "Resources_Completed_To_Date", 0)) as resource_1wa,
        -- sum(iff(week_ending = (select max(start_week) from intermediate.schedule.latest_date), "Resources_Completed_To_Date", 0)) as resource_end,
        "Resources_Completed_To_Date" as resource_end
    from 
    proj_start_weeks as a 
    left join 
    {{ source('P6', 'P6_WEEKLY_SUBTOTALS') }} as b -- wagram 
    on a.p6_proj_id = b.proj_id and a.start_week = b.week_ending 
    -- group by proj_id, week_ending
), resources_cost as (
    select 
        a.*,
        b.resource_end - b.resource_6wa as resources_used_6_weeks,
    from field_labor as a 
    left join 
    resources_used as b 
    on a.p6_proj_id = b.proj_id and a.start_week = b.week_ending
)
, cpi as (
    select 
        *,
        div0(resources_used_6_weeks, total_labor_cost) as six_week_cpi 
    from resources_cost
), next_week_resources as (
    select 
        a.*,
        b.week_ending,
        b."Next_Week_Resources" as next_week_resources
    from cpi as a 
    left join 
    {{ source('P6', 'P6_WEEKLY_SUBTOTALS') }} as b
    on a.p6_proj_id = b.proj_id and a.start_week = b.week_ending
    -- where b.week_ending = (select max(START_WEEK) from {{ ref('latest_date') }})
), resource_look_ahead as (
    select 
        p6_proj_id as proj_id,
        P6_full_project_name,
        week_ending,
        six_week_cpi,
        current_labor_cost,
        next_week_resources,
        div0((current_labor_cost * six_week_cpi), next_week_resources) * 100 as resource_look_ahead_pct
    from next_week_resources
    
), resource_look_ahead_grade as (
    select 
        proj_id,
        p6_full_project_name as proj_name,
        week_ending as start_week,
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
    where start_week is not null
    qualify row_number() over (PARTITION by proj_id, start_week order by proj_id, start_week) = 1
)
select * from resource_look_ahead_grade
where start_week is not null
order by start_week desc, proj_id
-- where proj_id = '21803'
-- order by proj_id, week_ending desc
 
-- select * from resources_used
-- where proj_id = '21675'
-- order by week_ending desc, proj_id