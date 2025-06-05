with budget as (
    select 
        bd.GLCo as          gl_co,
        bd.GLAcct as        gl_account, 
        ac.part1 as         natural_account,
        ac.part2 as         account_division,
        ac.part3 as         account_department,
        ac.Description as   account_description,
        ac.AcctType as      account_type,
        bd.BUDGETCODE as    budget_code, 
        bc.Description as   budget_description,
        bd.Mth as           mth,
        bd.BudgetAmt as     budget_amount
	from 
	{{ source('shookdw', 'GLBD') }} as bd 
	left join 
	{{ source('shookdw', 'GLAC') }} as ac
	using(GLCO, GLACCT)
    left join 
    {{ source('shookdw', 'GLBC') }} as bc
    using(GLCo, BUDGETCODE)
    where Mth >= '2015-01-01'
), division_add as (
    select 
        bd.*,
        dm.Description as department_description
    from 
        budget as bd 
    left join 
    {{ source('shookdw', 'bjcdm') }} as dm
    on trim(bd.account_division) = trim(dm.department)
), forecast_details as (
    select 
        *,
        split_part(budget_code, ' ', 1) as budget_year,
        REGEXP_SUBSTR(budget_code, 'FC([0-9.]+)', 1, 1, 'e', 1) AS fc_number
    from 
    division_add 
)

select 
    gl_co,
    gl_account, 
    natural_account,
    account_division,
    account_department,
    department_description,
    account_description,
    account_type,
    budget_code, 
    budget_description,
    budget_year,
    fc_number,
    mth,
    budget_amount
from forecast_details

