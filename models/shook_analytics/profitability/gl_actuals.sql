WITH
    gl_account_summary AS (
        SELECT 
            trim(glco) as glco
            ,trim(glacct) as glacct
            ,cast(mth as date) as mth
            ,netamt
        FROM 
            {{ source('shookdw', 'GLAS') }}
        WHERE
            year(cast(mth as date)) >= 2020
        -- WHERE
        --     year(cast(Mth as date)) = 2025
        --     and
        --     month(cast(Mth as date)) <= 5
        -- ORDER BY 
        --     cast(Mth as date)
    ),
    gl_accounts AS (
        SELECT DISTINCT
            trim(glco) as glco
            ,trim(glacct) as glacct
            ,trim(part1) as part1
            ,trim(part2) as part2
            ,trim(part3) as part3
            ,trim(description) as description
            ,trim(accttype) as accttype
        FROM {{ source('shookdw', 'GLAC') }}
    ),
    divisions as (
        SELECT DISTINCT
            trim(department) as department
            ,trim(glco) as glco
            ,trim(description) as description
        FROM 
            {{ source('shookdw', 'bjcdm') }}
    ),
    glpi_part1_desc as (
        SELECT DISTINCT
            trim(instance) as instance
            ,trim(glco) as glco
            ,trim(description) as description
        FROM 
            {{ source('shookdw', 'GLPI') }}
        WHERE
            partno = 1
    ),
    glpi_part2_desc as (
        SELECT DISTINCT
            trim(instance) as instance
            ,trim(glco) as glco
            ,trim(description) as description
        FROM 
            {{ source('shookdw', 'GLPI') }}
        WHERE
            partno = 2
    ),
    glpi_part3_desc as (
        SELECT DISTINCT
            trim(instance) as instance
            ,trim(glco) as glco
            ,trim(description) as description
        FROM 
            {{ source('shookdw', 'GLPI') }}
        WHERE
            partno = 3
    ),
    add_context_to_gl as (
        SELECT
            gl_summary.*
            ,gl_accts.part1 as natural_account
            ,gl_accts.part2 as account_division
            ,gl_accts.part3 as account_department
            ,gl_accts.description as account_description
            ,gl_accts.accttype as account_type
            ,gl_p1.description as natural_account_desc
            ,coalesce(divisions.description, gl_p2.description) as division_description
            ,gl_p3.description as department_description
            ,CASE
                WHEN gl_accts.part2 = '' THEN 0
                ELSE CAST(gl_accts.part2 as int)
            END AS account_division_number
            ,CASE
                WHEN gl_accts.part1 = '' THEN 0
                ELSE CAST(gl_accts.part1 as int)
            END AS natural_account_number
        FROM 
            gl_account_summary as gl_summary 
            LEFT JOIN gl_accounts as gl_accts
                on gl_summary.glco = gl_accts.glco and gl_summary.glacct = gl_accts.glacct
            LEFT JOIN divisions as divisions
                on gl_summary.glco = divisions.glco and gl_accts.part2 = divisions.department
            LEFT JOIN glpi_part1_desc as gl_p1
                on gl_summary.glco = gl_p1.glco and gl_accts.part1 = gl_p1.instance
            LEFT JOIN glpi_part2_desc as gl_p2
                on gl_summary.glco = gl_p2.glco and gl_accts.part2 = gl_p2.instance
            LEFT JOIN glpi_part3_desc as gl_p3
                on gl_summary.glco = gl_p3.glco and gl_accts.part3 = gl_p3.instance
    ),
    add_pl_context AS (
        SELECT
            *
            ,CASE 
                WHEN glacct like '41010%' or glacct like '41020%' or glacct like '41030%' then 'construction_revenue'
                WHEN glacct like '5%' then 'direct_construction_cost'
                WHEN glacct like '6%' or glacct like '7%' then 'indirect_construction_cost'
                WHEN glacct like '8%' and glacct not like '89%' and glacct not like '88200%' and glacct not like '88300%' and glacct not like '88400%'  then 'admin_expenses'
                -- WHEN natural_account_number >= 11000 and natural_account_number <= 11199 and glacct not like '11150%' and glacct not like '11170%' then 'cash'
                WHEN natural_account_number between 11000 and 11199 and natural_account_number not in (11150, 11170) and glco not in ('132', '134') then 'cash'
                ELSE 'other'
            END AS pl_line_item
            ,CASE
                WHEN glco not in ('41','47','51','54','56','58','61','64','67','120','122','124','126','128','130','132','134') 
                    and
                    (cast(glco as int) < 71 or cast(glco as int) > 104) then 1
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
-- SELECT 
--     pl_line_item, sum(netamt)
-- FROM 
--     add_pl_context
-- WHERE
--     include_company = 1
--     and
--     year(cast(Mth as date)) = 2025
--     and
--     month(cast(Mth as date)) <= 5
--     and
--     region = 'Midwest'
-- GROUP BY pl_line_item

-- SELECT pl_line_item, sum(netamt)
select *
FROM use_indirect_cost