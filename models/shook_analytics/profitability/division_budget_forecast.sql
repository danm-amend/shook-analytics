with budget as (
    select 
        *
    from 
        {{ ref('budget_biyearly') }}
    where 
        account_division in (
            select department
            from {{ source('shookdw', 'bjcdm') }}
            where lower(Description) like '%region%' -- make this into a table  
        ) 

), fc_type as (

    select 
        *,
        case
            when budget_year != EXTRACT(YEAR FROM TO_DATE(mth)) 
                then concat(extract(year from to_date(mth)),' Plan') 
            else budget_code
        end as budget_type,
        EXTRACT(YEAR FROM TO_DATE(mth)) as budget_plan_year
    from 
        budget
), account_agg as (
    select 
        gl_co,
        gl_account,
        natural_account,
        account_division,
        account_department,
        department_description,
        account_description,
        budget_type,
        budget_plan_year,
        sum(budget_amount) as budget_amount
    from 
        fc_type 
    group by 
        gl_co,
        gl_account,
        natural_account,
        account_division,
        account_department,
        department_description,
        account_description,
        budget_type,
        budget_plan_year
), account_rev as (
    select 
        * 
    from 
        account_agg
    where natural_account = '41010'
), account_margin as (
    select 
        gl_co,
        account_division,
        department_description,
        budget_type,
        budget_plan_year,
        sum(budget_amount) as budget_amount
    from 
        account_agg
    group by 
        gl_co,
        account_division,
        department_description,
        budget_type,
        budget_plan_year
), rev_margin as (
    select 
        mg.*,
        round(abs(rv.budget_amount), 2) as revenue,
        round(abs(mg.budget_amount), 2) as margin
    from
        account_rev as rv 
    join 
        account_margin as mg 
    using(gl_co, account_division, budget_type) 
), add_div_region as (
    select 
        gl_co,
        account_division,
        LPAD(LEFT(account_division, 2) || '00', 4, '0') AS region,
        RIGHT(account_division, 2) AS division,
        department_description,
        budget_type,
        budget_plan_year,
        revenue,
        margin
    from rev_margin
    where account_division not like '%00'
)

select * from add_div_region
order by account_division, budget_type