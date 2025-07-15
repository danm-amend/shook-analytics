WITH
    costs_by_job_month as (
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
    base_wip_by_month AS (
        SELECT 
            costs.jcco
            ,costs.job
            ,costs.mth
            ,costs.cumulative_actual_cost
            ,costs.cumulative_projected_cost
            --,contracts.contract_amt_change
            ,SUM(contracts.contract_amt_change) OVER (PARTITION BY costs.jcco, costs.job ORDER BY costs.mth) AS current_contract_amt
        FROM 
            costs_by_job_month AS costs
            LEFT JOIN contracts_by_month AS contracts
                    ON costs.jcco = contracts.jcco
                    AND costs.job = contracts.contract
                    AND costs.mth = contracts.mth
    ),
    contra_jobs AS (
        SELECT DISTINCT
            jcco
            ,job
            ,mth
            ,LEFT(job, 7) AS non_contra_job
        FROM 
            base_wip_by_month
        WHERE
            job LIKE '%.001%'
    ),
    /*

    Once completed vs in progress logic is determined, contra jobs that are completed will need adjusted to match the WIP file rather than showing contract amount of 0

    Completed Contra:
    Contract Amt and Projected Cost = Cumulative Actual Cost

    */
    contra_jobs_adjusted AS (
        SELECT 
            -- contra.jcco AS contra_jcco
            -- ,contra.job AS contra_job
            -- ,contra.mth AS contra_mth
            -- ,contra.non_contra_job
            -- ,wip.jcco AS wip_jcco
            -- ,wip.job AS wip_job
            -- ,wip.mth AS wip_mth
            -- ,wip.current_contract_amt
            contra.jcco
            ,contra.job
            ,contra.mth
            ,wip.current_contract_amt * -1 AS contra_current_contract_amt
            ,wip.current_contract_amt * -1 AS contra_cumulative_projected_cost
        FROM
            contra_jobs AS contra
                LEFT JOIN base_wip_by_month AS wip
                    ON contra.jcco = wip.jcco AND contra.non_contra_job = wip.job AND contra.mth = wip.mth
    ), -- SELECT * FROM contra_jobs_contract_amt WHERE job = '123043.001' ORDER BY mth
    wip_contra_adjusted AS (
        SELECT
            wip.jcco
            ,wip.job
            ,wip.mth
            ,wip.cumulative_actual_cost
            ,COALESCE(contra.contra_cumulative_projected_cost, wip.cumulative_projected_cost) AS cumulative_projected_cost
            ,COALESCE(contra.contra_current_contract_amt, wip.current_contract_amt) AS current_contract_amt
        FROM
            base_wip_by_month AS wip
                LEFT JOIN contra_jobs_adjusted AS contra
                    ON wip.jcco = contra.jcco AND wip.job = contra.job AND wip.mth = contra.mth
    ),
    wip_calcs_1 AS (
        SELECT
            *
            ,CASE
                WHEN cumulative_projected_cost = 0 THEN 0
                ELSE cumulative_actual_cost / cumulative_projected_cost
            END AS percent_complete
        FROM
            wip_contra_adjusted
    ),
    wip_calcs_2 AS (
        SELECT 
            *
            ,current_contract_amt - cumulative_projected_cost AS projected_margin_dollars
            ,CASE
                WHEN current_contract_amt - cumulative_projected_cost < 0 THEN current_contract_amt - cumulative_projected_cost
                ELSE (current_contract_amt - cumulative_projected_cost) * percent_complete
            END AS cumulative_margin_dollars
        FROM 
            wip_calcs_1
    ),
    wip_calcs_3 AS (
        SELECT 
            *
            ,CASE
                WHEN current_contract_amt = 0 THEN 0
                ELSE projected_margin_dollars / current_contract_amt
            END AS projected_margin_pct
            ,cumulative_actual_cost + cumulative_margin_dollars AS cumulative_revenue
            ,current_contract_amt - (cumulative_actual_cost + cumulative_margin_dollars) AS backlog_revenue
            ,projected_margin_dollars - cumulative_margin_dollars AS backlog_margin
        FROM 
            wip_calcs_2
    ),
    wip_calcs_4 AS (
        SELECT
            *,
            cumulative_revenue - LAG(cumulative_revenue) OVER (PARTITION BY jcco, job ORDER BY mth) AS this_month_revenue
            ,cumulative_margin_dollars - LAG(cumulative_margin_dollars) OVER (PARTITION BY jcco, job ORDER BY mth) AS this_month_margin
            ,MAX(mth) OVER (PARTITION BY jcco, job) AS max_month
        FROM 
            wip_calcs_3
    )
SELECT 
    *
FROM 
    wip_calcs_4
WHERE
    job LIKE '1%'
    AND
    LEN(job) >= 7
    AND
    max_month >= DATEADD(month, -6, GETDATE())
ORDER BY
    jcco
    ,job
    ,mth