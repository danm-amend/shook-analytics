WITH
    gl_account_summary AS (
        SELECT 
            trim(glco) as glco
            ,trim(glacct) as glacct
            ,cast(mth as date) as mth
            ,netamt
        FROM {{ source('shookdw', 'GLAS') }}
        WHERE
            year(cast(Mth as date)) = 2025
            and
            month(cast(Mth as date)) <= 5
        ORDER BY 
            cast(Mth as date)
    ),
    gl_accounts AS (
        SELECT
            trim(glco) as glco
            ,trim(glacct) as glacct
            ,trim(part1) as natural_account
            ,trim(part2) as account_division
            ,trim(part3) as account_department
            ,trim(description) as account_description
            ,trim(accttype) as account_type
        FROM {{ source('shookdw', 'GLAC') }}
    ),
    divisions as (
        SELECT
            trim(department) as department
            ,trim(glco) as glco
            ,trim(description) as description
        FROM 
            {{ source('shookdw', 'bjcdm') }}
    ),
    glpi_part1_desc as (
        SELECT
            trim(instance) as instance
            ,trim(glco) as glco
            ,trim(description) as description
        FROM 
            {{ source('shookdw', 'GLPI') }}
        WHERE
            partno = 1
    ),
    glpi_part2_desc as (
        SELECT
            trim(instance) as instance
            ,trim(glco) as glco
            ,trim(description) as description
        FROM 
            {{ source('shookdw', 'GLPI') }}
        WHERE
            partno = 2
    ),
    glpi_part3_desc as (
        SELECT
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
            ,gl_accts.natural_account
            ,gl_accts.account_division
            ,gl_accts.account_department
            ,gl_accts.account_description
            ,gl_accts.account_type
            ,gl_p1.description as natural_account_desc
            ,coalesce(divisions.description, gl_p2.description) as division_description
            ,gl_p3.description as department_description
        FROM 
            gl_account_summary as gl_summary 
            LEFT JOIN gl_accounts as gl_accts
                on gl_summary.glco = gl_accts.glco and gl_summary.glacct = gl_accts.glacct
            LEFT JOIN divisions as divisions
                on gl_summary.glco = divisions.glco and gl_accts.account_division = divisions.department
            LEFT JOIN glpi_part1_desc as gl_p1
                on gl_summary.glco = gl_p1.glco and gl_accts.account_division = gl_p1.instance
            LEFT JOIN glpi_part2_desc as gl_p2
                on gl_summary.glco = gl_p2.glco and gl_accts.natural_account = gl_p2.instance
            LEFT JOIN glpi_part3_desc as gl_p3
                on gl_summary.glco = gl_p3.glco and gl_accts.account_department = gl_p3.instance
    ), --TODO: ***************** confirm these joins are working as intended then check PL line items by region
    add_pl_context AS (
        SELECT
            *
            ,CASE 
                WHEN glacct like '41010%' or glacct like '41020%' or glacct like '41030%' then 'construction_revenue'
                WHEN glacct like '5%' then 'direct_construction_cost'
                WHEN glacct like '6%' or glacct like '7%' then 'indirect_construction_cost'
                WHEN glacct like '8%' and glacct not like '89%' and glacct not like '88200%' and glacct not like '88300%' and glacct not like '88400%'  then 'admin_expenses'
                ELSE 'other'
            END AS pl_line_item
            ,CASE
                WHEN glco not in ('41','47','51','54','56','58','61','64','67','120','122','124','126','128','130','132','134') 
                    and
                    (cast(glco as int) < 71 or cast(glco as int) > 104) then 1
                ELSE 0
            END AS include_company
        FROM add_context_to_gl
            
    )
SELECT pl_line_item, sum(netamt)
FROM add_pl_context
WHERE division_description like '%Midwest%'
GROUP BY pl_line_item
    