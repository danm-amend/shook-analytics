with gl_account_summary as (
    select * from {{ ref('gl_account_summary') }}
    where lower(department_description) like '%region%'
), account_agg as (
    select 
        gl_co,
        gl_account,
        natural_account,
        account_division,
        account_department,
        department_description,
        account_description,
        mth,
        sum(net_amount) as net_amount
    from 
        gl_account_summary
    group by 
        gl_co,
        gl_account,
        natural_account,
        account_division,
        account_department,
        department_description,
        account_description,
        Mth
), account_rev as (
    select 
        gl_co,
        gl_account,
        natural_account,
        account_division,
        account_department,
        department_description,
        account_description,
        mth,
        sum(net_amount) as revenue
    from account_agg
    where natural_account like '41010%'
    group by 
        gl_co,
        gl_account,
        natural_account,
        account_division,
        account_department,
        department_description,
        account_description,
        mth
), account_margin as (
    select 
        gl_co, 
        account_division,
        mth,
        sum(net_amount) as margin
    from account_agg 
    group by 
        gl_co,
        account_division,
        mth 
), rev_margin as (
    select
        ar.gl_co,
        ar.gl_account,
        ar.natural_account,
        ar.account_division,
        ar.account_department,
        ar.department_description,
        ar.account_description,
        ar.mth,
        round((ar.revenue)) as revenue,
        round((am.margin)) as margin
    from 
    account_rev as ar 
    left join 
    account_margin as am 
    using (gl_co, account_division, mth)
), add_div_region as (
    select 
        gl_co,
        account_division,
        LPAD(LEFT(account_division, 2) || '00', 4, '0') AS region,
        RIGHT(account_division, 2) AS division,
        department_description,
        Mth,
        revenue,
        margin
    from rev_margin
    where account_division not like '%00'
    and Mth >= '2020-01-01'
)


select * from add_div_region