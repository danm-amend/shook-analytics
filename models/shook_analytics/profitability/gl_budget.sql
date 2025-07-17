with budget as (
    select 
        *
    from 
        {{ ref('budget_biyearly') }}
--     where 
--     account_division in (
--     '0400', '0410', '0420', '0430', '0440',
--     '0600', '0610', '0620', '0630', '0640',
--     '0800', '0810', '0820', '0830', '0840',
--     '1200', '1210', '1220', '1230', '1240'
-- )
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
),
add_context_to_gl AS (
    SELECT *
        ,CASE
            WHEN account_division = '' THEN 0
            ELSE CAST(account_division as int)
        END AS account_division_number
    FROM union_plan_fc
),
add_pl_context AS (
            SELECT
            *
            ,CASE 
                WHEN gl_account like '41010%' or gl_account like '41020%' or gl_account like '41030%' then 'construction_revenue'
                WHEN gl_account like '5%' then 'direct_construction_cost'
                WHEN gl_account like '6%' or gl_account like '7%' then 'indirect_construction_cost'
                WHEN gl_account like '8%' and gl_account not like '89%' and gl_account not like '88200%' and gl_account not like '88300%' and gl_account not like '88400%'  then 'admin_expenses'
                --WHEN natural_account_number >= 11000 and natural_account_number <= 11199 and gl_acct not like '11150%' and glacct not like '11170%' then 'cash'
                ELSE 'other'
            END AS pl_line_item
            ,CASE
                WHEN gl_co not in ('41','47','51','54','56','58','61','64','67','120','122','124','126','128','130','132','134') 
                    and
                    (cast(gl_co as int) < 71 or cast(gl_co as int) > 104) then 1
                ELSE 0
            END AS include_company
            ,CASE
                WHEN account_division_number >= 1200 and account_division_number <= 1240 then 'Midwest'
                WHEN account_division_number >= 400 and account_division_number <= 440 then 'Great Lakes'
                WHEN account_division_number >= 800 and account_division_number <= 840 then 'Mid-Atlantic'
                WHEN account_division_number >= 600 and account_division_number <= 640 then 'Central'
            END AS region
            ,CASE
                WHEN account_division in ('0410','0610','0810','1210') then 'Water'
                WHEN account_division in ('0430','0630','0830','1230') then 'Healthcare'
                WHEN account_division in ('0440','0640','0840','1240') then 'Industrial'
                WHEN account_division in ('0420','0620','0820','1220') then 'Education'
            END AS market
        FROM add_context_to_gl
), use_indirect_cost as (
        select 
            *,
            iff(
                pl_line_item = 'indirect_construction_cost' and account_division in ('0000', '3000', '0400', '0600', '0800', '1200'),
                1,
                0
            ) as use_indirect_cost
        from add_pl_context
    )
-- SELECT pl_line_item, budget_name, sum(budget_amount)
-- FROM add_pl_context
-- WHERE 
--     include_company = 1
--     and
--     year(cast(mth as date)) = 2025
--     and
--     month(cast(mth as date)) <= 5
-- GROUP BY pl_line_item, budget_name
-- ORDER BY budget_name, pl_line_item
SELECT *
FROM use_indirect_cost
