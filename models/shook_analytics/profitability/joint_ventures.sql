WITH
consolidation_companies AS (
    SELECT 
        hqco
        ,name
    FROM 
        shookdw.viewpoint.hqco
    WHERE
        LOWER(name) LIKE '%consolidation%'
),
-- gl_agg_by_month AS (
--     SELECT
--         glco
--         ,mth
--         ,pl_line_item
--         ,SUM(netamt) as amt
--     FROM 
--         shook_analytics.profitability.gl_actuals
--     WHERE
--         pl_line_item = 'construction_revenue' 
--         OR 
--         pl_line_item = 'direct_construction_cost'
--     GROUP BY
--         glco
--         ,mth
--         ,pl_line_item
-- ),
glco_bounds AS (
    SELECT 
        glco
        ,MIN(DATE_TRUNC('MONTH', CAST(mth AS DATE))) AS min_month
        ,MAX(DATE_TRUNC('MONTH', CAST(mth AS DATE))) AS max_month
    FROM 
        shook_analytics.profitability.gl_actuals
    GROUP BY 
        glco
),
current_glco_bounds AS (
    SELECT *
    FROM glco_bounds
    WHERE min_month >= DATE_TRUNC('MONTH', DATEADD(YEAR, -8, CURRENT_DATE))
), 
month_numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS month_offset
    FROM TABLE(GENERATOR(ROWCOUNT => 200))  -- enough months to cover any job span
),
glco_months AS (
    SELECT
        cgb.glco
        ,DATEADD(MONTH, mn.month_offset, cgb.min_month) AS mth
    FROM current_glco_bounds cgb
        JOIN month_numbers mn
            ON mn.month_offset <= DATEDIFF(MONTH, cgb.min_month, cgb.max_month)
),
gl_agg_pivot AS (
    SELECT
        glco
        ,mth
        --,SUM(CASE WHEN pl_line_item = 'construction_revenue' THEN netamt END) * -1 AS revenue
        ,SUM(CASE WHEN glacct LIKE '41010%' THEN netamt END) AS contract_revenue
        ,SUM(CASE WHEN glacct LIKE '41030%' THEN netamt END) AS overbillings_adjusted
        ,SUM(CASE WHEN pl_line_item = 'direct_construction_cost' THEN netamt END) AS cost
    FROM 
        shook_analytics.profitability.gl_actuals
    GROUP BY
        glco
        ,mth
)
SELECT
    M.glco
    ,M.mth
    ,A.contract_revenue
    ,SUM(A.contract_revenue) OVER (PARTITION BY M.glco ORDER BY M.mth) AS Cumulative_Contract_Revenue
    ,A.overbillings_adjusted
    ,SUM(A.overbillings_adjusted) OVER (PARTITION BY M.glco ORDER BY M.mth) AS Cumulative_Overbillings_Adjusted
    ,A.cost
    ,SUM(A.cost) OVER (PARTITION BY M.glco ORDER BY M.mth) AS Cumulative_Cost
FROM 
    glco_months AS M
        LEFT JOIN gl_agg_pivot AS A
            ON M.glco = A.glco AND M.mth = A.mth
WHERE M.glco = '136'
ORDER BY M.mth





