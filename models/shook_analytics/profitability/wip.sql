WITH
    -- job_cost_agg query sets up all possible months within a job
    costs_history as (
        SELECT 
            "JCCO" AS jcco
            ,"JOB" AS job
            ,CAST("cost_month" AS DATE) AS mth
            ,SUM("CumulativeActualCost") AS cumulative_actual_cost
            ,SUM("CumulativeProjected") AS cumulative_projected_cost
        FROM 
            shook_analytics.profitability.job_cost_agg            
        GROUP BY
            "JCCO"
            ,"JOB"
            ,CAST("cost_month" AS DATE)
    ),
    contracts_by_month AS (
        SELECT
            "JCCO" AS jcco
            ,"CONTRACT" AS contract
            ,DATEFROMPARTS(YEAR(CAST("CHANGE_DATE" AS DATE)), MONTH(CAST("CHANGE_DATE" AS DATE)), 1) AS mth
            ,SUM("CONTRACT_AMT") AS contract_amt_change
        FROM
            shook_analytics.profitability.contract_total_by_month
        GROUP BY
            "JCCO"
            ,"CONTRACT"
            ,DATEFROMPARTS(YEAR(CAST("CHANGE_DATE" AS DATE)), MONTH(CAST("CHANGE_DATE" AS DATE)), 1)
    ),
    overrides_by_month AS (
        SELECT
            "JCCO" AS jcco
            ,"CONTRACT" AS contract
            ,CAST(MTH AS DATE) AS mth
            ,NULLIF(SUM(CONTRACTAMT),0) AS contract_amt
            ,NULLIF(SUM(BILLEDAMT), 0) AS billed_amt
            ,NULLIF(SUM(PROJREVENUE),0) AS proj_revenue
            ,NULLIF(SUM(PROJCOST),0) AS proj_cost
            ,NULLIF(SUM(PROJOVERCOST),0) AS proj_over_cost
            ,NULLIF(SUM(ACTUALCOST),0) AS actual_cost
            ,NULLIF(SUM(CURRESTCOST),0) AS estimated_cost
        FROM 
            shookdw.viewpoint.brvJCCostRevenueOverride
        GROUP BY 
            "JCCO"
            ,"CONTRACT"
            ,CAST(MTH AS DATE)
    ),
    wip_history_0 AS (
        SELECT 
            costs.jcco
            ,costs.job
            ,costs.mth
            ,costs.cumulative_actual_cost
            ,costs.cumulative_projected_cost
            ,contracts.contract_amt_change
            ,SUM(contracts.contract_amt_change) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS current_contract_amt
            -- "or" prefix = "override"
            -- "cor" prefix = "cumulative override"
            ,overrides.contract_amt AS or_contract_amt
            ,SUM(overrides.contract_amt) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS cor_contract_amt
            ,overrides.billed_amt AS or_billed_amt
            ,SUM(overrides.billed_amt) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS cor_billed_amt
            ,overrides.proj_revenue AS or_proj_revenue
            ,SUM(overrides.proj_revenue) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS cor_proj_revenue
            ,overrides.proj_cost AS or_proj_cost
            ,SUM(overrides.proj_cost) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS cor_proj_cost
            ,overrides.proj_over_cost AS or_proj_over_cost
            ,SUM(overrides.proj_over_cost) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS cor_proj_over_cost
            ,overrides.actual_cost AS or_actual_cost
            ,SUM(overrides.actual_cost) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS cor_actual_cost
            ,overrides.estimated_cost AS or_estimated_cost
            ,SUM(overrides.estimated_cost) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS cor_estimated_cost
        FROM 
            costs_history AS costs
            LEFT JOIN contracts_by_month AS contracts
                    ON costs.jcco = contracts.jcco
                    AND costs.job = contracts.contract
                    AND costs.mth = contracts.mth
            LEFT JOIN overrides_by_month AS overrides
                    ON costs.jcco = overrides.jcco
                    AND costs.job = overrides.contract
                    AND costs.mth = overrides.mth
    ),
    wip_history_1 AS (
        SELECT
            jcco
            ,job
            ,mth
            ,ROUND(COALESCE(or_proj_revenue, current_contract_amt), 0) AS TJ_Contract_Amount
            ,ROUND(COALESCE(or_proj_over_cost, cumulative_projected_cost), 0) AS TJ_Cost
            ,ROUND(COALESCE(cor_actual_cost, cumulative_actual_cost), 0) AS JTD_Cost
            --,COALESCE(cor_billed_amt, ) AS JTD_Billings --TODO: join billings in
        FROM 
            wip_history_0
    ),
    wip_history_2 AS (
        SELECT
            *
            ,TJ_Contract_Amount - TJ_Cost AS TJ_Margin
            ,CASE 
                WHEN TJ_Cost = 0 THEN 0 
                ELSE JTD_Cost / TJ_Cost 
            END AS JTD_Percent_Complete
        FROM
            wip_history_1
    ),
    wip_history_3 AS (
        SELECT
            *
            ,CASE 
                WHEN TJ_Contract_Amount = 0 THEN 0 
                ELSE TJ_Margin / TJ_Contract_Amount 
            END AS TJ_Margin_Pct
            ,CASE
                WHEN TJ_Margin < 0 THEN TJ_Margin
                ELSE JTD_Percent_Complete * TJ_Margin
            END AS JTD_Margin
        FROM
            wip_history_2
    ),
    wip_history_4 AS (
        SELECT
            *
            ,JTD_Margin + JTD_Cost AS JTD_Revenue
        FROM
            wip_history_3
    ),
    wip_history_5 AS (
        SELECT
            *
            ,TJ_Contract_Amount - JTD_Revenue AS Backlog_Revenue
            ,TJ_Margin - JTD_Margin AS Backlog_Margin
        FROM
            wip_history_4
    )
SELECT
    jcco
    ,job
    ,mth
    ,TJ_Contract_Amount
    ,TJ_Cost
    ,TJ_Margin
    ,TJ_Margin_Pct
    ,JTD_Revenue
    ,JTD_Cost
    ,JTD_Margin
    ,JTD_Percent_Complete
    ,Backlog_Revenue
    ,Backlog_Margin
FROM 
    wip_history_5