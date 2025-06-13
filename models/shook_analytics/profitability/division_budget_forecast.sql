with budget as (
    select 
        *
    from 
        {{ ref('budget_biyearly') }}
    where 
    lower(department_description) like '%region%'
        -- account_division in (
        --     select department
        --     from {{ source('shookdw', 'bjcdm') }}
        --     where lower(Description) like '%region%' -- make this into a table  
        -- ) 

), fc_name as (

    select 
        *,
        case
            when budget_year != EXTRACT(YEAR FROM TO_DATE(mth)) 
                then concat(extract(year from to_date(mth)),' Plan') 
            else concat(to_varchar(budget_year), ' FC', to_varchar(round(fc_number))) 
        end as budget_name,
        EXTRACT(YEAR FROM TO_DATE(mth)) as mth_year
    from 
        budget
), fc_type as (
    select 
        *, 
    case 
            when budget_name like '%Plan%' then 'Plan'
            when budget_name like '%FC%' then 'FC'
            else NULL
        end as budget_type
    from fc_name 
), select_plan as (
    select 
        * 
    from
        fc_type 
    where budget_type = 'Plan'
    qualify row_number() over (partition by gl_account, budget_name, mth order by fc_number desc) = 1 
), fc_mapping as (
    select budget_name, fc_base, max(fc_number) as max_fc_number
    from (
        select 
            budget_name, 
            fc_number,
            floor(fc_number) as fc_base
        from fc_type
        where budget_type = 'FC'
        group by 
            budget_name,
            fc_number
    ) as a 
    group by budget_name, fc_base
), select_fc as (
    select 
        a.*
    from 
        fc_type as a 
    join 
        fc_mapping as b 
    on a.budget_name = b.budget_name 
    and a.fc_number = b.max_fc_number
    where a.budget_type = 'FC' 
), union_plan_fc as (
    select * 
    from select_plan
    union all
    select * 
    from select_fc 
), account_agg as (
    select 
        gl_co,
        gl_account,
        natural_account,
        account_division,
        account_department,
        department_description,
        account_description,
        budget_name,
        mth_year,
        mth,
        sum(budget_amount) as budget_amount
    from 
        -- fc_type
        union_plan_fc 
    group by 
        gl_co,
        gl_account,
        natural_account,
        account_division,
        account_department,
        department_description,
        account_description,
        budget_name,
        mth_year,
        Mth
), account_rev as (
    select 
        * 
    from 
        union_plan_fc
    where natural_account = '41010'
), account_margin as (
    select 
        gl_co,
        account_division,
        department_description,
        budget_name,
        mth_year,
        Mth,
        sum(budget_amount) as budget_amount
    from 
        account_agg
    group by 
        gl_co,
        account_division,
        department_description,
        budget_name,
        mth_year,
        Mth
), rev_margin as (
    select 
        mg.*,
        round((rv.budget_amount), 2) as revenue,
        round((mg.budget_amount), 2) as margin
    from
        account_rev as rv 
    join 
        account_margin as mg 
    using(gl_co, account_division, budget_name, Mth) 
), add_div_region as (
    select 
        gl_co,
        account_division,
        LPAD(LEFT(account_division, 2) || '00', 4, '0') AS region,
        RIGHT(account_division, 2) AS division,
        department_description,
        budget_name,
        mth_year,
        Mth,
        revenue,
        margin
    from rev_margin
    where account_division not like '%00'
)


select 
*
from 
add_div_region


